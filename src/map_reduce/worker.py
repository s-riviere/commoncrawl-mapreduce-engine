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
#   python3 src/map_reduce/worker.py -h <host> -p <port> -i <input> -o <output> -l <local_map>
#
# Examples:
#   python3 src/map_reduce/worker.py -h tp-1a201-02.enst.fr -p 54321 -i ~/slr207-group1-bis/input -o ~/slr207-group1-bis/output -l /tmp/slr207-group1-bis/map-outputs
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
import time
import urllib.request
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np

# Compiled bytes regex: single pass over raw bytes, no per-line split+isalnum
_WORD_RE = re.compile(rb'[A-Za-z0-9]+')

# Vectorised CRC32: applies zlib.crc32 over a numpy object array in one call,
# avoiding explicit Python for-loop overhead on the unique-words list.
_crc32_vec = np.frompyfunc(zlib.crc32, 1, 1)


STATUS_READY_FOR_TASK = "READY_FOR_TASK"
STATUS_TASK_FINISHED = "TASK_FINISHED"

TASK_MAP = "MAP"
TASK_REDUCE = "REDUCE"
TASK_WAIT = "WAIT"


def log(level, message):
    """Print timestamped log message with worker label."""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [WORKER] [{level}] {message}", flush=True)


def send_json_line(sock, payload):
    """Helper to send JSON-encoded messages over socket."""
    sock.sendall((json.dumps(payload) + "\n").encode("utf-8"))


class MapReduceWorker:
    """Encapsulates worker process state and lifecycle."""

    def __init__(self, host, port, input_dir, output_dir, local_map_dir, max_ssh=None):
        """Initialize worker with connection and directory parameters."""
        self.host = host
        self.port = port
        self.input_dir = os.path.expanduser(input_dir)
        self.output_dir = os.path.expanduser(output_dir)
        self.local_map_dir = os.path.expanduser(local_map_dir)
        # max_ssh=None means auto: resolved at REDUCE time to min(n_workers, 8).
        # Empirically determined: shuffle t_wall floors at max_ssh=6-8 on this
        # cluster; beyond 8 gives no further gain but adds thread overhead.
        self.max_ssh = max_ssh
        self.socket = None
        self.buffer = ""
        self._t_clean = 0.0

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
        """Send JSON-encoded message to master."""
        send_json_line(self.socket, payload)

    def _execute_map(self, task):
        """Execute MAP task: partition input file and write local partition files."""
        split_id   = task["split_id"]
        n_reducers = task["n_reducers"]
        url        = task.get("url")  # present when master was started with --crawl

        file_name = f"commoncrawl-{split_id:04d}.txt"
        file_path = os.path.join(self.input_dir, file_name)

        # ── On-demand download to /tmp if NFS file is absent ──────────────
        # Workers download their assigned split independently in parallel;
        # /tmp is local disk (no NFS quota).  The file is deleted after MAP
        # to keep /tmp tidy.
        tmp_path      = None   # set if we downloaded to /tmp
        t_download    = 0.0
        if not os.path.exists(file_path):
            if not url:
                log("ERROR", f"Input split missing and no URL in task: {file_path}")
                return
            tmp_path  = f"/tmp/commoncrawl-{split_id:04d}.txt"
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

        log("INFO", f"MAP start split={split_id} reducers={n_reducers} input={file_path}")
        os.makedirs(self.local_map_dir, exist_ok=True)

        # ── Phase 1: read raw bytes (no text decoding) ────────────────────
        _t_read = time.time()
        with open(file_path, "rb") as f:
            raw_data = f.read()
        t_io_read = time.time() - _t_read

        # ── Phase 2: tokenise + pre-aggregate (combine) ───────────────────
        # Single regex pass over bytes → no per-line split, no isalnum loop.
        # Then aggregate ALL occurrences into one dict first so that CRC32 is
        # called only once per *unique* word, not once per occurrence.
        _t_compute = time.time()

        tokens = _WORD_RE.findall(raw_data)          # list[bytes]

        # Pass A – count every occurrence (pure dict; no CRC32 yet)
        total_counts: dict[bytes, int] = {}
        for tok in tokens:
            key = tok.lower()
            total_counts[key] = total_counts.get(key, 0) + 1

        # Pass B – assign each unique word to a reducer via vectorised CRC32
        if total_counts:
            keys_arr  = np.array(list(total_counts.keys()), dtype=object)
            crc_arr   = _crc32_vec(keys_arr).astype(np.int64)  # shape (U,)
            rids_arr  = (crc_arr % n_reducers).astype(np.intp)

            # Build per-reducer output lists (one entry per unique word)
            buckets: list[list[str]] = [[] for _ in range(n_reducers)]
            counts_list = list(total_counts.values())
            for i, (key_b, count) in enumerate(zip(keys_arr, counts_list)):
                buckets[rids_arr[i]].append(f"{key_b.decode()}\t{count}\n")
        else:
            buckets = [[] for _ in range(n_reducers)]

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

        # Clean up /tmp download to avoid filling local disk
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)

    def _execute_reduce(self, task):
        """Execute REDUCE task: shuffle partitions and aggregate word counts."""
        reducer_id = task["reducer_id"]
        map_workers = task["map_workers"]

        log("INFO", f"REDUCE start reducer={reducer_id} map_workers={len(map_workers)}")

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
        MAX_PARALLEL_SSH = min(len(map_workers), self.max_ssh if self.max_ssh is not None else 8)
        remote_partition = os.path.join(self.local_map_dir, f"partition_{reducer_id}.txt")

        def fetch_partition(raw_ip):
            """Fetch one partition file via SSH; returns (elapsed, lines)."""
            worker_ip = raw_ip.replace("::ffff:", "", 1) if raw_ip.startswith("::ffff:") else raw_ip
            cmd = f"ssh {ssh_opts} {worker_ip} 'cat {remote_partition}'"
            t0 = time.time()
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
        with ThreadPoolExecutor(max_workers=min(len(map_workers), MAX_PARALLEL_SSH)) as ex:
            futures = {ex.submit(fetch_partition, ip): ip for ip in map_workers}
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
        os.makedirs(self.output_dir, exist_ok=True)
        output_file_path = os.path.join(self.output_dir, f"part-{reducer_id}.txt")
        t_io_write = time.time()
        with open(output_file_path, "w") as f:
            f.writelines(
                f"{key}\t{total}\n"
                for key, total in sorted(final_counts.items(), key=lambda x: x[1], reverse=True)
            )
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

        self._clean_local_dir()
        self.buffer = ""

        while True:
            ready_msg = {"status": STATUS_READY_FOR_TASK}
            self._send_json_line(ready_msg)
            
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
                self._execute_map(task)
                notification = {"status": STATUS_TASK_FINISHED}
                self._send_json_line(notification)
                ack = self.socket.recv(1024)
                if not ack:
                    log("WARN", "Master closed connection before ACK after MAP")
                    break
                
            elif task_type == TASK_REDUCE:
                self._execute_reduce(task)
                notification = {"status": STATUS_TASK_FINISHED}
                self._send_json_line(notification)
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
    parser.add_argument("--help", action="help", help="Show this help message and exit")
    args = parser.parse_args()

    worker = MapReduceWorker(
        host=args.host,
        port=args.port,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        local_map_dir=args.local_map_dir,
        max_ssh=args.max_ssh,
    )
    worker.run()
