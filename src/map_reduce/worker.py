#!/usr/bin/env python3
import importlib
import json
import signal
import socket
import sys
import threading

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 54321

# Cache loaded modules so we don't re-import from disk on every request.
_module_cache: dict = {}


def load_user_function(module_name: str, func_name: str):
    if module_name not in _module_cache:
        _module_cache[module_name] = importlib.import_module(module_name)
    return getattr(_module_cache[module_name], func_name)


def recv_message(conn: socket.socket) -> bytes:
    """Read a length-prefixed message: 4-byte big-endian length + payload."""
    header = b""
    while len(header) < 4:
        chunk = conn.recv(4 - len(header))
        if not chunk:
            raise ConnectionError("connection closed while reading header")
        header += chunk
    length = int.from_bytes(header, "big")
    data = b""
    while len(data) < length:
        chunk = conn.recv(min(65536, length - len(data)))
        if not chunk:
            raise ConnectionError("connection closed while reading payload")
        data += chunk
    return data


def send_message(conn: socket.socket, payload: bytes) -> None:
    """Send a length-prefixed message."""
    conn.sendall(len(payload).to_bytes(4, "big") + payload)


def handle_client(conn: socket.socket) -> None:
    try:
        raw = recv_message(conn)
        request = json.loads(raw.decode("utf-8"))
        task_type = request.get("task")
        data      = request.get("data")
        job_name  = request.get("job_name")
        # Allow callers to pass bare name (e.g. "wordcount") or full name
        if job_name and "." not in job_name:
            job_name = f"map_reduce.{job_name}"

        if task_type == "MAP":
            user_map = load_user_function(job_name, "mapper")
            response = {"status": "OK", "result": user_map(data)}
        elif task_type == "REDUCE":
            user_reduce = load_user_function(job_name, "reducer")
            response = {"status": "OK", "result": user_reduce(data["key"], data["values"])}
        else:
            response = {"status": "ERROR", "message": "Unknown task"}

        send_message(conn, json.dumps(response).encode("utf-8"))
    except Exception as e:
        try:
            send_message(conn, json.dumps({"status": "ERROR", "message": str(e)}).encode("utf-8"))
        except OSError:
            pass
    finally:
        conn.close()


def main():
    srv = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    srv.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('::', PORT))
    srv.listen(64)

    def _shutdown(signum, frame):
        srv.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    print(f"Worker listening on port {PORT}...")
    while True:
        try:
            conn, _ = srv.accept()
            # One thread per connection — allows parallel tasks from master.
            threading.Thread(target=handle_client, args=(conn,), daemon=True).start()
        except OSError:
            pass


if __name__ == '__main__':
    main()
