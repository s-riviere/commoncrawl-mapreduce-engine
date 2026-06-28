#!/usr/bin/env python3

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# MapReduce worker process.
# Connects to the master, executes MAP and REDUCE tasks, and writes outputs.
#
# Arguments:
#   -h, --host           : Master hostname or IP.
#   -p, --port           : Master listening port.
#   -i, --input-dir      : Shared input directory containing split files.
#   -o, --output-dir     : Shared output directory for reduce results.
#   -l, --local-map-dir  : Local directory for intermediate MAP partitions.
#
# Usage :
#   python3 src/mapreduce/worker.py -h <host> -p <port> -i <input> -o <output> -l <local_map>
#
# Examples:
#   python3 src/mapreduce/worker.py -h tp-1a201-02.enst.fr -p 54321 -i ~/slr207-group1-bis/input -o ~/slr207-group1-bis/output -l /tmp/slr207-group1-bis/map-outputs
# ==============================================================================

import argparse
import collections
import gzip
import json
import os
import re
import shutil
import socket
import subprocess
import threading
import time
import urllib.request
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed

# numpy is an OPTIONAL accelerator: it vectorises the CRC32 reducer assignment.
# When it is absent (e.g. a fresh machine with no pip install), we fall back to
# an equivalent pure-Python loop so the engine still runs everywhere.
try:
    import numpy as np
    _crc32_vec = np.frompyfunc(zlib.crc32, 1, 1)
    _HAVE_NUMPY = True
except ImportError:  # pragma: no cover - portability fallback
    np = None
    _crc32_vec = None
    _HAVE_NUMPY = False

# Compiled bytes regex: single pass over raw bytes, no per-line split+isalnum
_WORD_RE = re.compile(rb'[A-Za-z0-9]+')


STATUS_READY_FOR_TASK = "READY_FOR_TASK"
STATUS_TASK_FINISHED = "TASK_FINISHED"
STATUS_HEARTBEAT = "HEARTBEAT"

TASK_MAP = "MAP"
TASK_REDUCE = "REDUCE"
TASK_WAIT = "WAIT"

# Worker → master liveness ping interval. Must be < master LEASE_TIMEOUT.
HEARTBEAT_INTERVAL = 2.0

# ── Language identification (job="lang") ─────────────────────────────────────
# Tiny per-language stop-word sets. A token that is a stop-word in exactly one
# language is a strong signal for that language; we tally those hits so the
# REDUCE phase ranks which languages dominate the crawl. Sets are intentionally
# small and high-frequency to keep the lookup cheap on millions of tokens.
_STOPWORDS = {
    "en": b"the of and to in is that it for as was with on are be this by from at or an",
    "fr": b"le la les des une dans est que qui pour pas avec sur ne se ce au aux du",
    "de": b"der die und den von zu mit auf ist nicht ein eine dem im des auch sich",
    "es": b"que los las una por con para del como una pero mas este esta son",
    "it": b"che non con per una sono come piu anche nella degli sulla dei alla",
}
# Build {token(bytes) -> lang}; tokens shared by several languages are dropped
# so they don't bias the count.
_STOPWORD_LANG: dict[bytes, bytes] = {}
_seen_multi: set[bytes] = set()
for _lang, _words in _STOPWORDS.items():
    for _w in _words.split():
        if _w in _STOPWORD_LANG and _STOPWORD_LANG[_w] != _lang.encode():
            _seen_multi.add(_w)
        else:
            _STOPWORD_LANG[_w] = _lang.encode()
for _w in _seen_multi:
    _STOPWORD_LANG.pop(_w, None)


def log(level, message):
    """Print timestamped log message with worker label."""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [WORKER] [{level}] {message}", flush=True)


def send_json_line(sock, payload):
    """Helper to send JSON-encoded messages over socket."""
    sock.sendall((json.dumps(payload) + "\n").encode("utf-8"))


class MapReduceWorker:
    """Encapsulates worker process state and lifecycle."""

    def __init__(self, host, port, input_dir, output_dir, local_map_dir, max_ssh=None, job="wordcount",
                 worker_id=None, advertise_host=None, local_shuffle=False,
                 direct_read=False, spill_dir="/tmp"):
        """Initialize worker with connection and directory parameters."""
        self.host = host
        self.port = port
        self.input_dir = os.path.expanduser(input_dir)
        self.output_dir = os.path.expanduser(output_dir)
        self.local_map_dir = os.path.expanduser(local_map_dir)
        self.job = job
        # §8 direct-read: stream the .wet.gz straight from Common Crawl (S3/HTTPS)
        # into memory at MAP time — no NFS file, no /tmp staging. Eliminates the
        # NFS intermediate that saturates under hundreds of concurrent reads.
        self.direct_read = bool(direct_read)
        # §2 spill dir: base for transient on-disk staging (the NFS-absent
        # download fallback). Point it at a large scratch partition (discovered
        # with src/benchmarks/find_scratch.sh) instead of the small default /tmp.
        self.spill_dir = os.path.expanduser(spill_dir)
        # Unique identity: lets several workers run on the SAME host (solo/local
        # test mode). Defaults to host:pid, which is unique per machine on the
        # real cluster, so production deployments behave exactly as before.
        self.worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}"
        # Host the master should use to reach us for the REDUCE remote-read.
        # None → the master uses our TCP peer address (the cluster default).
        self.advertise_host = advertise_host
        # In local/solo mode every worker is on this machine, so reducers read
        # map partitions straight from the filesystem (no SSH daemon needed).
        _loopback = {"127.0.0.1", "localhost", "::1"}
        self.local_shuffle = bool(local_shuffle) or (advertise_host in _loopback)
        # max_ssh=None means auto: resolved at REDUCE time to min(n_workers, 8).
        # Empirically determined: shuffle t_wall floors at max_ssh=6-8 on this
        # cluster; beyond 8 gives no further gain but adds thread overhead.
        self.max_ssh = max_ssh
        self.socket = None
        self.buffer = ""
        self._t_clean = 0.0
        # Heartbeat machinery: a background thread pings the master so it can
        # tell "slow task" apart from "dead worker". A lock serialises the two
        # writer threads (main loop + heartbeat) on the shared socket.
        self._send_lock = threading.Lock()
        self._stop = threading.Event()

    def _clean_local_dir(self):
        """Reset local MAP partitions directory between runs."""
        log("INFO", f"Cleaning local map directory: {self.local_map_dir}")
        t0 = time.time()
        if os.path.exists(self.local_map_dir):
            shutil.rmtree(self.local_map_dir)
        os.makedirs(self.local_map_dir, exist_ok=True)
        self._t_clean = time.time() - t0
        log("INFO", f"Local map directory ready: {self.local_map_dir} ({self._t_clean:.2f}s)")

    def _send_json_line(self, payload):
        """Send JSON-encoded message to master (thread-safe).

        On a lost connection (e.g. the master wrongly judged us dead during a
        long GIL-bound MAP), flag a clean shutdown instead of letting the
        exception crash the process: the master has already reclaimed our
        tasks, so there is nothing left to do but exit quietly.
        """
        with self._send_lock:
            if self._stop.is_set():
                return
            try:
                send_json_line(self.socket, payload)
            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                if not self._stop.is_set():
                    log("WARN", f"Lost connection to master "
                                f"({e.__class__.__name__}); shutting down cleanly.")
                self._stop.set()

    def _heartbeat_loop(self):
        """Background liveness ping so the master can detect a dead worker."""
        while not self._stop.wait(HEARTBEAT_INTERVAL):
            try:
                self._send_json_line({"status": STATUS_HEARTBEAT})
            except OSError:
                break

    @staticmethod
    def _map_emit(job, raw_data):
        """Tokenise raw bytes and emit (key -> local count) for the chosen job.

        Returns (tokens, counts) where ``counts`` maps a bytes key to its count
        in this split. Every job reduces to the same (key, count) shape, so the
        SHUFFLE/REDUCE path (sum by key, sort desc) is identical for all of them.
        """
        tokens = _WORD_RE.findall(raw_data)  # list[bytes]
        counts: dict[bytes, int] = {}

        if job == "wordlen":
            # "size": distribution of word lengths (key = length as text).
            for tok in tokens:
                key = str(len(tok)).encode()
                counts[key] = counts.get(key, 0) + 1
        elif job == "bigram":
            # "popularity" of phrases: frequency of consecutive word pairs.
            prev = None
            for tok in tokens:
                low = tok.lower()
                if prev is not None:
                    key = prev + b" " + low
                    counts[key] = counts.get(key, 0) + 1
                prev = low
        elif job == "lang":
            # "ranking": which languages dominate, via stop-word hits.
            for tok in tokens:
                lang = _STOPWORD_LANG.get(tok.lower())
                if lang is not None:
                    counts[lang] = counts.get(lang, 0) + 1
        else:  # "wordcount" (default): classic word frequency.
            for tok in tokens:
                key = tok.lower()
                counts[key] = counts.get(key, 0) + 1

        return tokens, counts

    @staticmethod
    def _strip_wet_headers(line: bytes) -> bool:
        """True if a WET line is a WARC/HTTP header (to drop), False if content."""
        return line.startswith((b"WARC/", b"CONTENT-", b"Content-", b"Metadata-"))

    def _stream_crawl_bytes(self, url: str) -> bytes:
        """§8: stream a .wet.gz directly from Common Crawl (S3/HTTPS) into memory.

        Decompresses on the fly and drops WARC/HTTP header lines so the in-memory
        bytes match exactly what the on-disk split format would contain — no NFS
        file and no /tmp staging are touched.
        """
        req = urllib.request.Request(url, headers={"User-Agent": "SLR207-MapReduce/1.0"})
        chunks: list[bytes] = []
        with urllib.request.urlopen(req, timeout=120) as resp:
            with gzip.open(resp, "rb") as gz_in:
                for line in gz_in:
                    if not self._strip_wet_headers(line):
                        chunks.append(line)
        return b"".join(chunks)

    def _execute_map(self, task):
        """Execute MAP task: partition input file and write local partition files."""
        split_id   = task["split_id"]
        n_reducers = task["n_reducers"]
        url        = task.get("url")  # present when master was started with --crawl
        job        = task.get("job", self.job)  # master chooses the analysis

        file_name = f"commoncrawl-{split_id:04d}.txt"
        file_path = os.path.join(self.input_dir, file_name)

        tmp_path      = None   # set if we staged a download on disk
        t_download    = 0.0
        raw_data      = None   # set directly by the §8 direct-read path

        # ── §8 direct-read: stream straight from Common Crawl (no NFS/no /tmp) ─
        if self.direct_read and url:
            log("INFO", f"MAP split={split_id}: direct-read streaming from {url}")
            _t_dl = time.time()
            try:
                raw_data = self._stream_crawl_bytes(url)
                t_download = time.time() - _t_dl
                log("INFO", f"MAP split={split_id}: streamed {len(raw_data)} bytes in {t_download:.1f}s")
            except Exception as e:
                log("ERROR", f"MAP split={split_id}: direct-read failed: {e}")
                return
        # ── On-demand download to the spill dir if the NFS file is absent ─────
        # Workers download their assigned split independently in parallel; the
        # spill dir is local disk (no NFS quota). Deleted after MAP to stay tidy.
        elif not os.path.exists(file_path):
            if not url:
                log("ERROR", f"Input split missing and no URL in task: {file_path}")
                return
            os.makedirs(self.spill_dir, exist_ok=True)
            tmp_path  = os.path.join(self.spill_dir, f"commoncrawl-{split_id:04d}.txt")
            log("INFO", f"MAP split={split_id}: NFS file absent, downloading to {tmp_path}")
            _t_dl = time.time()
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "SLR207-MapReduce/1.0"})
                with urllib.request.urlopen(req, timeout=120) as resp:
                    with gzip.open(resp, "rt", encoding="utf-8", errors="ignore") as gz_in:
                        with open(tmp_path, "w", encoding="utf-8") as f_out:
                            for line in gz_in:
                                f_out.write(line)
                t_download = time.time() - _t_dl
                file_path  = tmp_path
                log("INFO", f"MAP split={split_id}: download done in {t_download:.1f}s")
            except Exception as e:
                log("ERROR", f"MAP split={split_id}: download failed: {e}")
                return

        src_label = "stream" if raw_data is not None else file_path
        log("INFO", f"MAP start split={split_id} reducers={n_reducers} input={src_label}")
        os.makedirs(self.local_map_dir, exist_ok=True)

        # ── Phase 1: read raw bytes (no text decoding) ────────────────────
        # Direct-read already holds the bytes in memory; otherwise read the file.
        _t_read = time.time()
        if raw_data is None:
            with open(file_path, "rb") as f:
                raw_data = f.read()
        t_io_read = time.time() - _t_read

        # ── Phase 2: tokenise + pre-aggregate (combine) ───────────────────
        # Single regex pass over bytes → no per-line split, no isalnum loop.
        # Then aggregate ALL occurrences into one dict first so that CRC32 is
        # called only once per *unique* word, not once per occurrence.
        _t_compute = time.time()

        # Single regex pass over bytes; the per-job keying lives in _map_emit.
        # Pass A produces ALL (key, count) pairs so CRC32 is called only once
        # per *unique* key below, not once per occurrence.
        tokens, total_counts = self._map_emit(job, raw_data)

        # Pass B - assign each unique word to a reducer via CRC32.
        # numpy path vectorises the CRC32 over all unique keys at once; the
        # pure-Python fallback computes the same crc32(key) % n_reducers per key.
        buckets: list[list[str]] = [[] for _ in range(n_reducers)]
        if total_counts:
            if _HAVE_NUMPY:
                keys_arr  = np.array(list(total_counts.keys()), dtype=object)
                crc_arr   = _crc32_vec(keys_arr).astype(np.int64)  # shape (U,)
                rids_arr  = (crc_arr % n_reducers).astype(np.intp)
                counts_list = list(total_counts.values())
                for i, (key_b, count) in enumerate(zip(keys_arr, counts_list)):
                    buckets[rids_arr[i]].append(f"{key_b.decode()}\t{count}\n")
            else:
                for key_b, count in total_counts.items():
                    rid = zlib.crc32(key_b) % n_reducers
                    buckets[rid].append(f"{key_b.decode()}\t{count}\n")

        t_compute = time.time() - _t_compute

        # ── Phase 3: write – one line per unique word, batched ────────────
        t_write = time.time()
        for rid, lines in enumerate(buckets):
            if not lines:
                continue
            partition_path = os.path.join(self.local_map_dir, f"partition_{rid}.txt")
            with open(partition_path, "a") as f_out:
                f_out.writelines(lines)
        t_write = time.time() - t_write

        t_map_total = getattr(self, '_t_clean', 0.0) + t_io_read + t_compute + t_write
        log("INFO",
            f"MAP done split={split_id} "
            f"t_download={t_download:.2f}s "
            f"t_clean={getattr(self, '_t_clean', 0):.2f}s "
            f"t_io_read={t_io_read:.2f}s "
            f"t_compute={t_compute:.2f}s "
            f"t_io_write={t_write:.2f}s "
            f"t_total={t_map_total:.2f}s "
            f"unique_words={len(total_counts)} tokens={len(tokens)}")
        print(
            f"WORKER_TIMING: {{\"phase\":\"MAP\",\"split_id\":{split_id},"
            f"\"t_download\":{t_download:.3f},"
            f"\"t_clean\":{getattr(self, '_t_clean', 0):.3f},"
            f"\"t_io_read\":{t_io_read:.3f},"
            f"\"t_compute\":{t_compute:.3f},"
            f"\"t_io_write\":{t_write:.3f}}}",
            flush=True)

        # Clean up the spill-dir download to avoid filling local disk
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)

    def _execute_reduce(self, task):
        """Execute REDUCE task: shuffle partitions and aggregate word counts."""
        reducer_id = task["reducer_id"]
        # Each source is {"host": h, "map_dir": d}: the machine that ran a MAP and
        # the directory where it wrote its partitions. Older masters only sent a
        # list of hosts in "map_workers"; fall back to that, using this reducer's
        # own local_map_dir as the (shared) path.
        sources = task.get("map_sources")
        if sources is None:
            sources = [{"host": h, "map_dir": None} for h in task.get("map_workers", [])]

        log("INFO", f"REDUCE start reducer={reducer_id} sources={len(sources)} local_shuffle={self.local_shuffle}")

        # ControlMaster=auto: first SSH to each host creates a multiplexed master
        # socket; all subsequent connections (including parallel ones from other
        # reducers on the same machine) reuse it transparently.  The server only
        # sees ONE real TCP connection per unique host regardless of parallelism,
        # which avoids triggering fail2ban / IDS on the cluster.
        ctrl_path = f"/tmp/ssh-ctrl-%h-{self.port}"
        ssh_opts = (
            "-o StrictHostKeyChecking=no "
            "-o BatchMode=yes "
            "-o LogLevel=ERROR "
            "-o ControlMaster=auto "
            f"-o ControlPath={ctrl_path} "
            "-o ControlPersist=120s"
        )
        # Empirically determined optimum (ssh_parallelism_sweep.py):
        # shuffle wall-time floors at max_ssh=6-8 for N≤32 on this cluster.
        # ControlMaster deduplicates real TCP connections, so going higher is
        # free on the network but adds thread overhead with no shuffle benefit.
        MAX_PARALLEL_SSH = min(max(len(sources), 1), self.max_ssh if self.max_ssh is not None else 8)

        def fetch_partition(source):
            """Fetch one partition file (local read or SSH); returns (elapsed, lines)."""
            host    = source.get("host")
            map_dir = source.get("map_dir") or self.local_map_dir
            part    = os.path.join(map_dir, f"partition_{reducer_id}.txt")
            t0 = time.time()
            lines = []
            if self.local_shuffle:
                # Same machine (solo/local mode): read the partition straight
                # from the local filesystem - no SSH daemon required.
                try:
                    with open(part, "r") as f:
                        lines = f.readlines()
                except FileNotFoundError:
                    lines = []
                except Exception as e:
                    log("ERROR", f"REDUCE reducer={reducer_id} local read error path={part}: {e}")
                return time.time() - t0, lines
            worker_ip = host.replace("::ffff:", "", 1) if host and host.startswith("::ffff:") else host
            cmd = f"ssh {ssh_opts} {worker_ip} 'cat {part}'"
            try:
                proc = subprocess.Popen(
                    cmd, shell=True,
                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True
                )
                lines = proc.stdout.readlines()
                proc.wait(timeout=30)
                if proc.returncode != 0:
                    log("WARN", f"REDUCE reducer={reducer_id} ssh failed host={worker_ip} code={proc.returncode}")
            except Exception as e:
                log("ERROR", f"REDUCE reducer={reducer_id} shuffle error host={worker_ip}: {e}")
                lines = []
            return time.time() - t0, lines

        # ── Phase: parallel shuffle ────────────────────────────────────────
        # All SSH fetches run concurrently; wall time = slowest single fetch.
        t_shuffle_start = time.time()
        all_raw_lines = []
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_SSH) as ex:
            futures = {ex.submit(fetch_partition, s): s for s in sources}
            for fut in as_completed(futures):
                _, lines = fut.result()
                all_raw_lines.extend(lines)
        t_shuffle = time.time() - t_shuffle_start

        # ── Phase: aggregate ──────────────────────────────────────────────
        t_compute_start = time.time()
        final_counts: dict[str, int] = {}
        for line in all_raw_lines:
            if not line.strip():
                continue
            k, v = line.split("\t", 1)
            final_counts[k] = final_counts.get(k, 0) + int(v)
        t_compute = time.time() - t_compute_start

        # ── Phase: write output (I/O) ─────────────────────────────────────
        # ── Phase: write output (I/O) ────────────────────────────────────────
        # Atomic commit: write to a per-reducer .tmp on the SAME directory, then
        # os.replace() (atomic rename) into place. A reducer that dies mid-write
        # therefore never leaves a half-written part-*.txt, and a re-executed
        # reducer cleanly overwrites the previous attempt (exactly-once output).
        os.makedirs(self.output_dir, exist_ok=True)
        output_file_path = os.path.join(self.output_dir, f"part-{reducer_id}.txt")
        tmp_path = f"{output_file_path}.{os.getpid()}.tmp"
        t_io_write = time.time()
        with open(tmp_path, "w") as f:
            f.writelines(
                f"{key}\t{total}\n"
                for key, total in sorted(final_counts.items(), key=lambda x: x[1], reverse=True)
            )
        os.replace(tmp_path, output_file_path)
        t_io_write = time.time() - t_io_write

        log("INFO",
            f"REDUCE done reducer={reducer_id} "
            f"t_shuffle={t_shuffle:.2f}s "
            f"t_compute={t_compute:.2f}s "
            f"t_io_write={t_io_write:.2f}s")
        print(
            f"WORKER_TIMING: {{\"phase\":\"REDUCE\",\"reducer_id\":{reducer_id},"
            f"\"t_shuffle\":{t_shuffle:.3f},"
            f"\"t_compute\":{t_compute:.3f},"
            f"\"t_io_write\":{t_io_write:.3f}}}",
            flush=True)

    def run(self):
        """Main worker loop: connect, get tasks, execute, report completion."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.socket.connect((self.host, self.port))
            log("INFO", f"Connected to master {self.host}:{self.port}")
        except Exception as e:
            log("ERROR", f"Failed to connect to master {self.host}:{self.port}: {e}")
            return

        # Register identity so the master can (a) tell workers apart even when
        # several run on the SAME host (solo/local mode) and (b) learn each
        # worker's local map directory for the REDUCE remote-read.
        self._send_json_line({
            "status": "REGISTER",
            "worker_id": self.worker_id,
            "host": self.advertise_host,
            "map_dir": self.local_map_dir,
        })

        self._clean_local_dir()
        self.buffer = ""

        # Start the liveness heartbeat once the socket is up.
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        while not self._stop.is_set():
            ready_msg = {"status": STATUS_READY_FOR_TASK}
            self._send_json_line(ready_msg)
            if self._stop.is_set():
                break

            task_line = None
            while True:
                data = self.socket.recv(4096).decode('utf-8')
                if not data:
                    log("WARN", "Master connection closed while waiting for task")
                    break
                self.buffer += data
                if "\n" in self.buffer:
                    task_line, self.buffer = self.buffer.split("\n", 1)
                    break
            
            if not task_line:
                log("WARN", "No task received, stopping worker loop")
                break
                
            task = json.loads(task_line)
            task_type = task.get("type")
            log("INFO", f"Received task type={task_type}")
            
            if task_type == TASK_MAP:
                # Ping right before the long GIL-bound compute so the master's
                # lease clock restarts from task start.
                self._send_json_line({"status": STATUS_HEARTBEAT})
                self._execute_map(task)
                self._send_json_line({"status": STATUS_TASK_FINISHED})
                if self._stop.is_set():
                    break
                ack = self.socket.recv(1024)
                if not ack:
                    log("WARN", "Master closed connection before ACK after MAP")
                    break
                
            elif task_type == TASK_REDUCE:
                self._send_json_line({"status": STATUS_HEARTBEAT})
                self._execute_reduce(task)
                self._send_json_line({"status": STATUS_TASK_FINISHED})
                if self._stop.is_set():
                    break
                ack = self.socket.recv(1024)
                if not ack:
                    log("WARN", "Master closed connection before ACK after REDUCE")
                    break
                
            elif task_type == TASK_WAIT:
                time.sleep(2)
                # Defensive reset: avoid stale fragments if the sender interleaves bursts.
                self.buffer = ""

            else:
                log("WARN", f"Unknown task type received: {task_type}")

        self._stop.set()
        self.socket.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MapReduce Worker", add_help=False)
    parser.add_argument("-h", "--host", required=True, metavar="HOST", help="Master IP address or hostname")
    parser.add_argument("-p", "--port", required=True, type=int, metavar="PORT", help="Master listening port")
    parser.add_argument("-i", "--input-dir", required=True, metavar="DIR", help="Shared input directory for splits")
    parser.add_argument("-o", "--output-dir", required=True, metavar="DIR", help="Shared output directory for reduce results")
    parser.add_argument("-l", "--local-map-dir", required=True, metavar="DIR", help="Local directory for MAP intermediate partitions")
    parser.add_argument("--max-ssh", type=int, default=None, metavar="N",
                        help="Max parallel SSH connections during REDUCE shuffle "
                             "(default: auto = min(n_workers, 8), empirically optimal)")
    parser.add_argument("-j", "--job", default="wordcount",
                        choices=["wordcount", "lang", "wordlen", "bigram"], metavar="JOB",
                        help="Analysis to run (default: wordcount). Overridden by the per-task job from the master.")
    parser.add_argument("--worker-id", default=None, metavar="ID",
                        help="Unique worker identity (default: hostname:pid). Pass a distinct value "
                             "per worker to run several workers on the SAME machine (solo/local mode).")
    parser.add_argument("--advertise-host", default=None, metavar="HOST",
                        help="Host the master should use to reach this worker for the REDUCE remote-read "
                             "(default: the TCP peer address). Use 127.0.0.1 for local/solo mode.")
    parser.add_argument("--local-shuffle", action="store_true",
                        help="Read MAP partitions from the local filesystem instead of over SSH "
                             "(auto-enabled when --advertise-host is loopback). For single-machine testing.")
    parser.add_argument("--direct-read", action="store_true",
                        help="Stream each split's .wet.gz straight from Common Crawl (S3/HTTPS) into "
                             "memory at MAP time — no NFS file and no on-disk staging (day4 §2).")
    parser.add_argument("--spill-dir", default="/tmp", metavar="DIR",
                        help="Base directory for transient on-disk staging (NFS-absent download "
                             "fallback). Point at a large scratch partition (see src/benchmarks/find_scratch.sh) "
                             "instead of the default /tmp.")
    parser.add_argument("--help", action="help", help="Show this help message and exit")
    args = parser.parse_args()

    worker = MapReduceWorker(
        host=args.host,
        port=args.port,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        local_map_dir=args.local_map_dir,
        max_ssh=args.max_ssh,
        job=args.job,
        worker_id=args.worker_id,
        advertise_host=args.advertise_host,
        local_shuffle=args.local_shuffle,
        direct_read=args.direct_read,
        spill_dir=args.spill_dir,
    )
    try:
        worker.run()
    except (BrokenPipeError, ConnectionResetError, OSError) as e:
        log("WARN", f"Worker stopped: lost connection to master ({e.__class__.__name__}).")
