#!/usr/bin/env python3

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# MapReduce master server.
# Orchestrates MAP and REDUCE phases by distributing tasks to workers,
# collecting task completions, and stopping when the job is finished.
#
# Arguments:
#   -p, --port      : Listening port for worker connections.
#   -s, --splits    : Number of MAP splits to process.
#   -r, --reducers  : Number of REDUCE tasks.
#
# Usage :
#   python3 src/map_reduce/master.py -p <port> -s <splits> -r <reducers>
#
# Examples:
#   python3 src/map_reduce/master.py -p 54321 -s 10 -r 10
#   python3 src/map_reduce/master.py -p 60000 -s 20 -r 12
# ==============================================================================

import argparse
import collections
import gzip
import json
import socket
import threading
import time
import urllib.request

BASE_CC_URL    = "https://data.commoncrawl.org/"
DEFAULT_CRAWL  = "CC-MAIN-2024-10"


STATUS_READY_FOR_TASK = "READY_FOR_TASK"
STATUS_TASK_FINISHED = "TASK_FINISHED"
STATUS_HEARTBEAT = "HEARTBEAT"
STATUS_REGISTER = "REGISTER"
STATUS_ACK = "ACK"

TASK_MAP = "MAP"
TASK_WAIT = "WAIT"
TASK_REDUCE = "REDUCE"
TASK_SHUTDOWN = "SHUTDOWN"

# ── Fault-tolerance tunables ─────────────────────────────────────────────────
# Workers send a HEARTBEAT every HEARTBEAT_INTERVAL seconds.  The master arms a
# socket recv() timeout of LEASE_TIMEOUT on each worker connection: if no byte
# (heartbeat, READY or TASK_FINISHED) arrives within the lease, the worker is
# declared DEAD and its tasks are reclaimed.  This detects both a killed
# process (TCP closes immediately) and a network partition (lease expires).
HEARTBEAT_INTERVAL = 2.0   # worker → master, seconds
LEASE_TIMEOUT = 10.0       # master declares a worker dead after this silence
# A still-running MAP task older than this is eligible for a backup copy on an
# otherwise-idle worker (Google MapReduce §3.6 straggler mitigation).
STRAGGLER_THRESHOLD = 30.0


def log(level, message):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [MASTER] [{level}] {message}", flush=True)


def normalize_worker_host(host):
    """Convert IPv4-mapped IPv6 addresses (::ffff:a.b.c.d) to plain IPv4."""
    if host.startswith("::ffff:"):
        return host.replace("::ffff:", "", 1)
    return host


class MasterServer:
    def __init__(self, port, n_splits, n_reducers, crawl_id=None, job="wordcount"):
        self.port = port
        self.n_splits = n_splits
        self.n_reducers = n_reducers
        self.crawl_id = crawl_id  # if set, embed download URLs in MAP tasks
        self.job = job            # which map/reduce logic the workers run
        self._wet_paths: list[str] = []  # populated by _fetch_wet_paths()

        self.lock = threading.Lock()
        self.phase = TASK_MAP

        # ── MAP bookkeeping (fault-tolerant) ────────────────────────────────
        self.map_specs: dict[int, dict] = {}            # split_id -> task spec
        self.map_queue: list[int] = []                  # pending split_ids
        self.completed_splits: set[int] = set()         # split_ids done & alive
        self.map_done_by = collections.defaultdict(set) # worker_id -> {split_id}
        self.map_inflight_since: dict[int, tuple] = {}  # split_id -> (worker_id, t0)
        self.backed_up: set[int] = set()                # split_ids with a backup

        # ── REDUCE bookkeeping ──────────────────────────────────────────────
        self.reduce_queue: list[int] = []
        self.completed_reducers: set[int] = set()

        # ── Shared ──────────────────────────────────────────────────────────
        # Workers are identified by a unique worker_id (not just their host), so
        # several workers can run on the SAME machine -- this is what makes the
        # single-machine "solo" test mode possible. worker_info maps each id to
        # the {host, map_dir} a reducer needs to remote-read its partitions.
        self.worker_info: dict[str, dict] = {}          # worker_id -> {host, map_dir}
        self.inflight: dict[str, tuple] = {}            # worker_id -> (type, task_id)
        self.map_holder_ids: set[str] = set()           # worker_ids holding partitions
        self.alive_workers: set[str] = set()
        self.total_map_tasks = 0
        self.total_reduce_tasks = 0
        self.job_completed = False

        # Timing (wall-clock, seconds)
        self.t_start = None
        self.t_map_end = None
        self.t_reduce_end = None

    def _send_json(self, conn, payload):
        conn.sendall((json.dumps(payload) + "\n").encode("utf-8"))

    def _fetch_wet_paths(self):
        """Fetch the WET file path list for the configured crawl."""
        url = f"{BASE_CC_URL}crawl-data/{self.crawl_id}/wet.paths.gz"
        log("INFO", f"Fetching WET paths from {url} ...")
        req = urllib.request.Request(url, headers={"User-Agent": "SLR207-MapReduce/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            with gzip.open(resp, "rt") as f:
                self._wet_paths = [line.strip() for line in f if line.strip()]
        log("INFO", f"Loaded {len(self._wet_paths)} WET paths for crawl {self.crawl_id}")

    def load_tasks(self):
        if self.crawl_id:
            self._fetch_wet_paths()

        for i in range(self.n_splits):
            spec = {"type": TASK_MAP, "split_id": i, "n_reducers": self.n_reducers, "job": self.job}
            if self._wet_paths and i < len(self._wet_paths):
                spec["url"] = BASE_CC_URL + self._wet_paths[i]
            self.map_specs[i] = spec

        self.map_queue = list(range(self.n_splits))
        self.total_map_tasks = self.n_splits

        self.reduce_queue = list(range(self.n_reducers))
        self.total_reduce_tasks = self.n_reducers
        log("INFO", f"Tasks loaded: {self.total_map_tasks} MAP, {self.total_reduce_tasks} REDUCE (job={self.job})")

    def _pick_backup_split(self, wid):
        """Return a still-running MAP split eligible for a backup copy, or None.

        Straggler mitigation (Google MapReduce §3.6): when the MAP queue is
        empty but some tasks are still in flight, an idle worker is handed a
        *duplicate* of the slowest in-flight split.  Whichever copy finishes
        first wins; the loser's late TASK_FINISHED is discarded as stale.
        """
        now = time.time()
        best, best_elapsed = None, STRAGGLER_THRESHOLD
        for split, (owner, t0) in self.map_inflight_since.items():
            if split in self.completed_splits or split in self.backed_up:
                continue
            if owner == wid:
                continue
            elapsed = now - t0
            if elapsed > best_elapsed:
                best, best_elapsed = split, elapsed
        if best is not None:
            self.backed_up.add(best)
        return best

    def _dispatch(self, conn, wid):
        # Lock held by caller: queue pops and phase checks stay consistent.
        if self.phase == TASK_MAP:
            if self.map_queue:
                split = self.map_queue.pop(0)
                self.inflight[wid] = (TASK_MAP, split)
                self.map_inflight_since[split] = (wid, time.time())
                log("INFO", f"{wid} starts MAP {split}")
                self._send_json(conn, dict(self.map_specs[split]))
                return
            backup = self._pick_backup_split(wid)
            if backup is not None:
                self.inflight[wid] = (TASK_MAP, backup)
                log("INFO", f"{wid} starts BACKUP MAP {backup} (straggler mitigation)")
                spec = dict(self.map_specs[backup])
                spec["backup"] = True
                self._send_json(conn, spec)
                return
            self._send_json(conn, {"type": TASK_WAIT})
            return

        if self.phase == TASK_REDUCE:
            if self.reduce_queue:
                rid = self.reduce_queue.pop(0)
                self.inflight[wid] = (TASK_REDUCE, rid)
                sources = self._map_sources()
                log("INFO", f"{wid} starts REDUCE {rid}")
                self._send_json(conn, {
                    "type": TASK_REDUCE,
                    "reducer_id": rid,
                    "map_sources": sources,
                    "map_workers": [s["host"] for s in sources],
                    "job": self.job,
                })
            else:
                self._send_json(conn, {"type": TASK_WAIT})

    def _map_sources(self):
        """Build the list of {host, map_dir} a reducer must remote-read from."""
        sources = []
        for w in self.map_holder_ids:
            info = self.worker_info.get(w, {"host": w, "map_dir": None})
            sources.append({"host": info.get("host", w), "map_dir": info.get("map_dir")})
        return sources

    def _finish(self, conn, wid):
        # Always ACK first so the worker can proceed; then update bookkeeping.
        rec = self.inflight.pop(wid, None)
        self._send_json(conn, {"status": STATUS_ACK})
        if rec is None:
            return  # stale finish (e.g. a backup loser, or a cancelled reduce)
        typ, tid = rec

        if typ == TASK_MAP and self.phase == TASK_MAP:
            if tid in self.completed_splits:
                log("INFO", f"{wid} finished MAP {tid} (duplicate/backup, ignored)")
                return
            self.completed_splits.add(tid)
            self.map_done_by[wid].add(tid)
            self.map_holder_ids.add(wid)
            self.map_inflight_since.pop(tid, None)
            log("INFO", f"{wid} finished MAP {tid} ({len(self.completed_splits)}/{self.total_map_tasks})")
            if len(self.completed_splits) == self.total_map_tasks:
                self.t_map_end = time.time()
                self.phase = TASK_REDUCE
                self.map_holder_ids = set(self.map_done_by.keys())
                log("INFO", f"MAP phase complete; switching to REDUCE (sources={len(self.map_holder_ids)})")

        elif typ == TASK_REDUCE and self.phase == TASK_REDUCE:
            if tid in self.completed_reducers:
                return
            self.completed_reducers.add(tid)
            log("INFO", f"{wid} finished REDUCE {tid} ({len(self.completed_reducers)}/{self.total_reduce_tasks})")
            if len(self.completed_reducers) == self.total_reduce_tasks:
                self.t_reduce_end = time.time()
                self.job_completed = True
                log("INFO", "REDUCE phase complete; job finished")
        # else: a finish that no longer matches the current phase → stale, ignored.

    def _reclaim(self, wid):
        """Reclaim the tasks of a worker that has just died (lock held).

        Re-execution rules (Google MapReduce §3.1):
          * in-progress task on a dead worker → re-queue it;
          * COMPLETED map on a dead worker    → re-run it (its /tmp output is
            gone with the machine);
          * COMPLETED reduce                  → kept (output was committed
            atomically to NFS, see worker._execute_reduce).
        """
        self.alive_workers.discard(wid)
        if self.job_completed:
            return

        rec = self.inflight.pop(wid, None)
        if rec is not None:
            typ, tid = rec
            if typ == TASK_MAP:
                self.map_inflight_since.pop(tid, None)
                self.backed_up.discard(tid)
                if tid not in self.completed_splits and tid not in self.map_queue:
                    self.map_queue.append(tid)
            elif typ == TASK_REDUCE:
                if tid not in self.completed_reducers and tid not in self.reduce_queue:
                    self.reduce_queue.append(tid)

        # Map outputs held by this worker are now unreachable → re-run them.
        lost = self.map_done_by.pop(wid, set())
        self.map_holder_ids.discard(wid)
        if not lost:
            return

        log("WARN", f"{wid} DIED holding {len(lost)} completed MAP output(s) → re-running {sorted(lost)}")
        for split in lost:
            self.completed_splits.discard(split)
            self.map_inflight_since.pop(split, None)
            self.backed_up.discard(split)
            if split not in self.map_queue:
                self.map_queue.append(split)

        if self.phase == TASK_REDUCE:
            # Shuffle sources changed: fall back to MAP, then redo every REDUCE
            # (reduce outputs are atomic, so recomputing them is safe/idempotent).
            log("WARN", "A MAP source died during REDUCE → reverting to MAP and re-queueing all REDUCE tasks")
            self.phase = TASK_MAP
            self.t_map_end = None
            for h, (t, _tid) in list(self.inflight.items()):
                if t == TASK_REDUCE:
                    self.inflight.pop(h, None)
            self.completed_reducers.clear()
            self.reduce_queue = list(range(self.total_reduce_tasks))

    def handle_worker(self, conn, addr):
        peer_host = normalize_worker_host(addr[0])
        # Worker identity: a worker REGISTERs with a unique worker_id (so several
        # workers may share one host in solo/local mode). Until then we fall back
        # to the peer host (legacy workers that never send REGISTER still work).
        wid = None
        # Arm the failure detector: if no heartbeat/message arrives within the
        # lease, recv() raises socket.timeout and we declare the worker dead.
        conn.settimeout(LEASE_TIMEOUT)
        log("INFO", f"Worker connected: {peer_host}")

        def ensure_identity(message=None):
            """Resolve and register this connection's worker_id (lock held)."""
            nonlocal wid
            if message is not None and message.get("status") == STATUS_REGISTER:
                wid = message.get("worker_id") or peer_host
                host = message.get("host") or peer_host
                self.worker_info[wid] = {"host": host, "map_dir": message.get("map_dir")}
                self.alive_workers.add(wid)
                log("INFO", f"Worker registered: id={wid} host={host} map_dir={message.get('map_dir')}")
                return
            if wid is None:
                wid = peer_host
                self.worker_info.setdefault(wid, {"host": peer_host, "map_dir": None})
                self.alive_workers.add(wid)

        # recv() can return partial JSON chunks; keep a per-connection buffer.
        buffer = ""

        while True:
            try:
                data = conn.recv(4096).decode("utf-8")
            except socket.timeout:
                log("WARN", f"Lease expired for {wid or peer_host} (no heartbeat for {LEASE_TIMEOUT:.0f}s) → DEAD")
                break
            except OSError:
                break
            if not data:
                break

            buffer += data
            try:
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if not line.strip():
                        continue

                    message = json.loads(line)
                    status = message.get("status")

                    if status == STATUS_REGISTER:
                        with self.lock:
                            ensure_identity(message)
                        continue
                    if status == STATUS_HEARTBEAT:
                        continue  # liveness only; the recv() reset the lease
                    elif status == STATUS_READY_FOR_TASK:
                        with self.lock:
                            ensure_identity()
                            self._dispatch(conn, wid)
                    elif status == STATUS_TASK_FINISHED:
                        with self.lock:
                            ensure_identity()
                            self._finish(conn, wid)
            except Exception as exc:
                log("ERROR", f"Worker {wid or peer_host} handler error: {exc}")
                break

        try:
            conn.close()
        except OSError:
            pass
        with self.lock:
            if wid is not None:
                self._reclaim(wid)
        log("INFO", f"Worker disconnected: {wid or peer_host}")

    def run(self):
        # Bind the socket BEFORE loading tasks so that workers can connect and
        # queue up while the master is still fetching wet.paths.gz.
        # Workers that connect early will receive TASK_WAIT until load_tasks()
        # finishes and populates self.map_tasks.
        server = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        server.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("::", self.port))
        server.listen(64)
        server.settimeout(1)
        log("INFO", f"Listening on port {self.port} (dual-stack), reducers={self.n_reducers}")

        self.load_tasks()  # may fetch wet.paths.gz; workers wait via TASK_WAIT
        log("INFO", "Tasks ready — accepting work")

        self.t_start = time.time()
        try:
            while True:
                with self.lock:
                    if self.job_completed:
                        break

                try:
                    conn, addr = server.accept()
                except socket.timeout:
                    continue

                thread = threading.Thread(target=self.handle_worker, args=(conn, addr), daemon=True)
                thread.start()
        except KeyboardInterrupt:
            pass
        finally:
            server.close()

        # Emit structured timing line for amdahl_bench.py to parse
        if self.t_start and self.t_reduce_end:
            t_total = self.t_reduce_end - self.t_start
            t_map   = (self.t_map_end - self.t_start) if self.t_map_end else 0.0
            t_reduce = (self.t_reduce_end - self.t_map_end) if self.t_map_end else t_total
            print(
                f"TIMING: {{\"t_total\": {t_total:.3f}, "
                f"\"t_map\": {t_map:.3f}, "
                f"\"t_reduce\": {t_reduce:.3f}}}",
                flush=True,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Master Server pour MapReduce")
    parser.add_argument("-p", "--port",     type=int, default=54321, help="Port d'ecoute (defaut: 54321)")
    parser.add_argument("-s", "--splits",   type=int, default=10,    help="Nombre de splits MAP (defaut: 10)")
    parser.add_argument("-r", "--reducers", type=int, default=10,    help="Nombre de reducers (defaut: 10)")
    parser.add_argument("-c", "--crawl",    default=None,
                        help=f"CommonCrawl crawl ID to embed download URLs in tasks (e.g. {DEFAULT_CRAWL}). "
                             "Workers download splits on-demand to /tmp if NFS file is missing.")
    parser.add_argument("-j", "--job", default="wordcount",
                        choices=["wordcount", "lang", "wordlen", "bigram"],
                        help="Analysis to run (default: wordcount). "
                             "lang=language popularity, wordlen=word-length distribution, bigram=phrase popularity.")
    args = parser.parse_args()

    master = MasterServer(
        port=args.port,
        n_reducers=args.reducers,
        n_splits=args.splits,
        crawl_id=args.crawl,
        job=args.job,
    )
    master.run()
