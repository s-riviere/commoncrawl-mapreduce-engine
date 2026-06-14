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

# ── Argument parsing ──────────────────────────────────────────────────────────
args = sys.argv[1:]
PORT = 54321
DO_SYNC = False
SYNC_HOST = None          # host from which to fetch machines.txt

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

SRC_PATH = Path(__file__).resolve().parent.parent
MACHINES_FILE = SRC_PATH / "runtime" / "machines.txt"
TIMEOUT = 5   # seconds per TCP connection attempt
SCP_OPTS = [
    '-4',
    '-o', 'StrictHostKeyChecking=no',
    '-o', 'ConnectTimeout=10',
    '-o', 'BatchMode=yes',
    '-o', 'LogLevel=ERROR',
]


# ── Sync from /tmp ────────────────────────────────────────────────────────────
def sync_machines(host: str) -> None:
    """Fetch machines.txt from HOST:/tmp/slr207-group1/machines.txt."""
    remote_path = f'{host}:/tmp/slr207-group1/machines.txt'
    print(f'[SYNC] Fetching machines.txt from {remote_path} ...')

    result = subprocess.run(
        ['scp'] + SCP_OPTS + [remote_path, MACHINES_FILE],
        capture_output=True,
    )
    if result.returncode == 0:
        print(f'[SYNC] OK — got machines.txt from {host}\n')
    else:
        print('[SYNC] Failed to fetch machines.txt.')
        print('       Possible causes:')
        print('         • SSH key not set up on that machine')
        print('         • Machine is unreachable')
        print(f'       Proceeding with local {MACHINES_FILE} (may be stale).\n')


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
        with open(MACHINES_FILE) as f:
            machines = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f'Error: {MACHINES_FILE} not found.')
        print('  Deploy first:  ./deploy.sh <port>')
        print('  Or sync:       python3 client.py <port> --sync')
        sys.exit(1)

    total = len(machines)
    print(f'Connecting to {total} machines on port {PORT}...\n')

    raw = []
    for m in machines:
        raw.append(query(m))
        print(f"Done: {m}")
        import time
        time.sleep(0.5)

    # Sort: reachable nodes first (alphabetical), unreachable at the bottom
    raw.sort(key=lambda r: (r[1] is None, r[0]))

    results, failed = [], []
    for host, l1, l5, l15 in raw:
        if l1 is None:
            failed.append(host)
            print(f'  {host:<35} UNREACHABLE')
        else:
            results.append((host, l1, l5, l15))
            print(f'  {host:<35} load: {l1:.2f}  {l5:.2f}  {l15:.2f}')

    n = len(results)
    print(f'\n{"=" * 55}')
    print(f'  Nodes responded: {n}/{total}')

    if n > 0:
        a1  = sum(r[1] for r in results) / n
        a5  = sum(r[2] for r in results) / n
        a15 = sum(r[3] for r in results) / n
        print(f'  Avg cluster load  1 min : {a1:.4f}')
        print(f'  Avg cluster load  5 min : {a5:.4f}')
        print(f'  Avg cluster load 15 min : {a15:.4f}')

    print('=' * 55)

    if failed:
        print(f'\n  Unreachable ({len(failed)}): {", ".join(failed)}')


if __name__ == '__main__':
    main()