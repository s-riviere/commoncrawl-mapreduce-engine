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
import json
import socket
import threading
import time


STATUS_READY_FOR_TASK = "READY_FOR_TASK"
STATUS_TASK_FINISHED = "TASK_FINISHED"
STATUS_ACK = "ACK"

TASK_MAP = "MAP"
TASK_WAIT = "WAIT"
TASK_REDUCE = "REDUCE"
TASK_SHUTDOWN = "SHUTDOWN"


def log(level, message):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [MASTER] [{level}] {message}", flush=True)


def normalize_worker_host(host):
    """Convert IPv4-mapped IPv6 addresses (::ffff:a.b.c.d) to plain IPv4."""
    if host.startswith("::ffff:"):
        return host.replace("::ffff:", "", 1)
    return host


class MasterServer:
    def __init__(self, port, n_splits, n_reducers):
        self.port = port
        self.n_splits = n_splits
        self.n_reducers = n_reducers

        self.lock = threading.Lock()
        self.map_tasks = []
        self.reduce_tasks = []

        self.phase = TASK_MAP
        self.completed_tasks = 0
        self.total_map_tasks = 0
        self.total_reduce_tasks = 0

        self.active_map_workers = set()
        self.active_connections = 0
        self.job_completed = False

        # Timing (wall-clock, seconds)
        self.t_start = None
        self.t_map_end = None
        self.t_reduce_end = None

    def _send_json(self, conn, payload):
        conn.sendall((json.dumps(payload) + "\n").encode("utf-8"))

    def load_tasks(self):
        self.map_tasks = [
            {"type": TASK_MAP, "split_id": i, "n_reducers": self.n_reducers}
            for i in range(self.n_splits)
        ]
        self.total_map_tasks = len(self.map_tasks)

        self.reduce_tasks = [{"type": TASK_REDUCE, "reducer_id": i} for i in range(self.n_reducers)]
        self.total_reduce_tasks = len(self.reduce_tasks)
        log("INFO", f"Tasks loaded: {self.total_map_tasks} MAP, {self.total_reduce_tasks} REDUCE")

    def _dispatch_task_for_worker(self, conn, worker_host):
        # The lock is held by caller: queue pops and phase checks stay consistent.
        if self.phase == TASK_MAP:
            if self.map_tasks:
                task = self.map_tasks.pop(0)
                log("INFO", f"{worker_host} starts MAP {task['split_id']}")
                self._send_json(conn, task)
            else:
                self._send_json(conn, {"type": TASK_WAIT})
            return

        if self.phase == TASK_REDUCE:
            if self.reduce_tasks:
                task = self.reduce_tasks.pop(0)
                task["map_workers"] = list(self.active_map_workers)
                log("INFO", f"{worker_host} starts REDUCE {task['reducer_id']}")
                self._send_json(conn, task)
            else:
                self._send_json(conn, {"type": TASK_WAIT})

    def _mark_task_finished(self, conn, worker_host):
        # Keep behavior: every finished task contributes to active_map_workers as in current protocol.
        log("INFO", f"{worker_host} finished a task")
        self.completed_tasks += 1
        self.active_map_workers.add(worker_host)
        self._send_json(conn, {"status": STATUS_ACK})

        if self.phase == TASK_MAP and self.completed_tasks == self.total_map_tasks:
            log("INFO", "MAP phase completed; switching to REDUCE")
            self.t_map_end = time.time()
            self.phase = TASK_REDUCE
            self.completed_tasks = 0
        elif self.phase == TASK_REDUCE and self.completed_tasks == self.total_reduce_tasks:
            log("INFO", "REDUCE phase completed; job finished")
            self.t_reduce_end = time.time()
            self.job_completed = True

    def handle_worker(self, conn, addr):
        worker_host = normalize_worker_host(addr[0])
        log("INFO", f"Worker connected: {worker_host}")

        with self.lock:
            self.active_connections += 1

        # recv() can return partial JSON chunks; keep a per-connection buffer.
        buffer = ""

        while True:
            try:
                data = conn.recv(4096).decode("utf-8")
                if not data:
                    break

                buffer += data
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if not line.strip():
                        continue

                    message = json.loads(line)
                    status = message.get("status")

                    if status == STATUS_READY_FOR_TASK:
                        with self.lock:
                            self._dispatch_task_for_worker(conn, worker_host)

                    elif status == STATUS_TASK_FINISHED:
                        with self.lock:
                            self._mark_task_finished(conn, worker_host)

            except Exception as exc:
                log("ERROR", f"Worker {worker_host} handler error: {exc}")
                break

        conn.close()
        with self.lock:
            self.active_connections -= 1
        log("INFO", f"Worker disconnected: {worker_host}")

    def run(self):
        self.load_tasks()

        server = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        server.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("::", self.port))
        server.listen(64)
        server.settimeout(1)
        log("INFO", f"Listening on port {self.port} (dual-stack), reducers={self.n_reducers}")

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
    parser.add_argument("-p", "--port", type=int, default=54321, help="Port d'ecoute (defaut: 54321)")
    parser.add_argument("-s", "--splits", type=int, default=10, help="Nombre de splits MAP (defaut: 10)")
    parser.add_argument("-r", "--reducers", type=int, default=10, help="Nombre de reducers (defaut: 10)")
    args = parser.parse_args()

    master = MasterServer(
        port=args.port,
        n_reducers=args.reducers,
        n_splits=args.splits
    )
    master.run()
