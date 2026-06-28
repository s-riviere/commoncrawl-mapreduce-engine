#!/usr/bin/env python3
"""
Load client — Challenge 1.
Connects to all deployed servers, prints per-node load averages and
cluster-wide averages (1, 5, 15 min).

Usage:
  python3 client.py [port]               use local machines.txt
  python3 client.py [port] --sync HOST   fetch machines.txt from HOST:/tmp/slr207-group1/
"""
import socket
import subprocess
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# ── Argument parsing ──────────────────────────────────────────────────────────
args = sys.argv[1:]
PORT = 54321
DO_SYNC = False
SYNC_HOST = None          

if args and not args[0].startswith('--'):
    PORT = int(args.pop(0))

if '--sync' in args:
    DO_SYNC = True
    idx = args.index('--sync')
    if idx + 1 < len(args) and not args[idx + 1].startswith('--'):
        SYNC_HOST = args[idx + 1]
    else:
        print('Error: --sync requires a HOST argument.')
        print('Usage: python3 client.py [port] --sync HOST')
        sys.exit(1)

LOCAL_MACHINES_FILE = str(Path(__file__).resolve().parent.parent.parent / "runtime" / "machines.txt")
REMOTE_MACHINES_FILE = '/tmp/slr207-group1-cpuload/machines.txt'
TIMEOUT = 5   
SCP_OPTS = [
    '-4',
    '-o', 'StrictHostKeyChecking=no',
    '-o', 'ConnectTimeout=5',
    '-o', 'BatchMode=yes',
    '-o', 'LogLevel=ERROR',
]

# ── Colors ────────────────────────────────────────────────────────────────────
NC = "\033[0m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"


# ── Sync from /tmp ────────────────────────────────────────────────────────────
def sync_machines(host: str) -> None:
    print("=================================================")
    print(f" Syncing machines.txt from {host}                ")
    print("=================================================")
    print("")
    print(f"    Fetching from {REMOTE_MACHINES_FILE} ... ", end="", flush=True)

    result = subprocess.run(
        ['scp'] + SCP_OPTS + [f'{host}:{REMOTE_MACHINES_FILE}', LOCAL_MACHINES_FILE],
        capture_output=True,
    ).returncode

    if result == 0:
        print(f"{GREEN}[OK]{NC}\n")
    else:
        print(f"{RED}[FAILED]{NC}")
        print("    Proceeding with local machines.txt (may be stale).\n")
    print("")


# ── Per-node TCP query ────────────────────────────────────────────────────────
def query(host: str) -> tuple:
    """Open a TCP connection to host:PORT and read the load triple."""
    try:
        for af, socktype, proto, _, sa in socket.getaddrinfo(
                host, PORT, type=socket.SOCK_STREAM):
            try:
                with socket.socket(af, socktype, proto) as s:
                    s.settimeout(TIMEOUT)
                    s.connect(sa)
                    chunks = []
                    while True:
                        chunk = s.recv(1024)
                        if not chunk:
                            break
                        chunks.append(chunk)
                data = b''.join(chunks).decode().strip()
                l1, l5, l15 = map(float, data.split())
                return host, l1, l5, l15
            except OSError:
                continue
    except Exception:
        pass
    return host, None, None, None


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    if DO_SYNC:
        sync_machines(SYNC_HOST)

    try:
        with open(LOCAL_MACHINES_FILE) as f:
            machines = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f'{RED}Error: {LOCAL_MACHINES_FILE} not found.{NC}')
        sys.exit(1)

    total = len(machines)
    
    print("=================================================")
    print(f" Querying Cluster Load on port {PORT}            ")
    print("=================================================")
    print("")

    # Query in parallel to avoid blocking sequential effect
    raw = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        raw = list(executor.map(query, machines))

    # Sort: reachable nodes first (alphabetical), unreachable at the bottom
    raw.sort(key=lambda r: (r[1] is None, r[0]))

    results, failed = [], []
    for host, l1, l5, l15 in raw:
        print(f"    Checking {host:<25} ", end="", flush=True)
        if l1 is None:
            failed.append(host)
            print(f"{RED}[UNREACHABLE]{NC}")
        else:
            results.append((host, l1, l5, l15))
            print(f"{GREEN}[ONLINE]{NC}  ({l1:.2f}  {l5:.2f}  {l15:.2f})")

    n = len(results)
    print("")
    print("=================================================")
    print(f" Final Report                                    ")
    print("=================================================")
    print("")
    print(f"    Nodes responded: {n}/{total}")

    if n > 0:
        a1  = sum(r[1] for r in results) / n
        a5  = sum(r[2] for r in results) / n
        a15 = sum(r[3] for r in results) / n
        print(f"    Avg cluster load  1 min : {a1:.4f}")
        print(f"    Avg cluster load  5 min : {a5:.4f}")
        print(f"    Avg cluster load 15 min : {a15:.4f}")

    print("")
    print("=================================================")


if __name__ == '__main__':
    main()
