#!/usr/bin/env python3
"""
master.py — Generic MapReduce orchestrator.

Usage:
  python3 -m map_reduce.master wordcount [port] [--num-files N] [--sync HOST]

  --num-files N   number of Common Crawl WET files to process (default: 10)
  --sync HOST     fetch machines.txt from HOST before starting
"""
import json
import socket
import sys
import gzip
import io
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
import map_reduce.cluster as cluster

TIMEOUT     = 180   # seconds — WET download inside worker can take a while
BASE_CC_URL = "https://data.commoncrawl.org/"
DEFAULT_CRAWL = "CC-MAIN-2024-10"


# ── Length-prefixed framing (matches worker.py) ───────────────────────────────

def _send_message(conn: socket.socket, payload: bytes) -> None:
    conn.sendall(len(payload).to_bytes(4, "big") + payload)


def _recv_message(conn: socket.socket) -> bytes:
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


# ── Network task dispatch ─────────────────────────────────────────────────────

def send_task(host: str, port: int, task_type: str,
              job_name: str, data) -> dict | None:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(TIMEOUT)
            s.connect((host, port))
            payload = json.dumps({"task": task_type,
                                  "job_name": job_name,
                                  "data": data}).encode("utf-8")
            _send_message(s, payload)
            return json.loads(_recv_message(s).decode("utf-8"))
    except Exception as e:
        print(f"  [WARN] {host}:{port} → {e}", file=sys.stderr)
        return None


# ── Common Crawl URL list ─────────────────────────────────────────────────────

def get_wet_urls(num_files: int, crawl: str = DEFAULT_CRAWL) -> list[str]:
    """Fetch the WET paths index and return the first `num_files` full URLs."""
    index_url = f"{BASE_CC_URL}crawl-data/{crawl}/wet.paths.gz"
    print(f"[MASTER] Fetching WET index from {index_url} ...")
    req = urllib.request.Request(index_url,
                                 headers={"User-Agent": "SLR207-MapReduce/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
    with gzip.open(io.BytesIO(raw), "rt") as f:
        paths = [line.strip() for line in f if line.strip()]
    urls = [BASE_CC_URL + p for p in paths[:num_files]]
    print(f"[MASTER] Selected {len(urls)} WET files.\n")
    return urls


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = sys.argv[1:]

    # Parse --num-files before handing the rest to cluster.parse_arguments
    num_files = 10
    if "--num-files" in args:
        idx = args.index("--num-files")
        num_files = int(args[idx + 1])
        args = args[:idx] + args[idx + 2:]

    job_name, port, sync_host = cluster.parse_arguments(args)
    machines = cluster.load_machines(sync_host)
    N = len(machines)

    if N == 0:
        print("Error: no workers available.")
        sys.exit(1)

    # ── Fixed dataset: always the same URLs regardless of N ───────────────────
    wet_urls = get_wet_urls(num_files)
    K = len(wet_urls)   # number of map tasks (= number of files)

    print(f"[MASTER] Job '{job_name}' | {N} workers | {K} map tasks | port {port}\n")

    # ── Phase MAP — parallel fan-out ──────────────────────────────────────────
    print("--- MAP phase ---")
    map_results: list[tuple[str, int]] = []

    with ThreadPoolExecutor(max_workers=N) as pool:
        future_to_url = {
            pool.submit(send_task,
                        machines[i % N], port, "MAP", job_name, wet_urls[i]): wet_urls[i]
            for i in range(K)
        }
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            res = future.result()
            if res and res.get("status") == "OK":
                map_results.extend(res["result"])
                print(f"  [OK]  {url.split('/')[-1]}")
            else:
                msg = res.get("message", "no response") if res else "no response"
                print(f"  [ERR] {url.split('/')[-1]} — {msg}", file=sys.stderr)

    if not map_results:
        print("Error: all map tasks failed.", file=sys.stderr)
        sys.exit(1)

    # ── Phase SHUFFLE (serial — runs on master) ────────────────────────────────
    print("\n--- SHUFFLE phase ---")
    shuffled: dict[str, list[int]] = {}
    for key, value in map_results:
        shuffled.setdefault(key, []).append(value)
    print(f"  {len(shuffled)} unique keys")

    # ── Phase REDUCE — parallel fan-out ───────────────────────────────────────
    print("\n--- REDUCE phase ---")
    final_results: dict[str, int] = {}
    items = list(shuffled.items())

    with ThreadPoolExecutor(max_workers=N) as pool:
        future_to_key = {
            pool.submit(send_task,
                        machines[i % N], port, "REDUCE", job_name,
                        {"key": key, "values": values}): key
            for i, (key, values) in enumerate(items)
        }
        for future in as_completed(future_to_key):
            res = future.result()
            if res and res.get("status") == "OK":
                r_key, r_val = res["result"]
                final_results[r_key] = r_val

    # ── Results ───────────────────────────────────────────────────────────────
    top = sorted(final_results.items(), key=lambda x: -x[1])[:50]
    print(f'\n{"=" * 55}\n RESULTS: {job_name} (top {len(top)} words)\n{"=" * 55}')
    for k, v in top:
        print(f'  {k:<30} {v}')
    print('=' * 55)
    print(f'  Total unique words : {len(final_results):,}')
    print('=' * 55)


if __name__ == '__main__':
    main()
