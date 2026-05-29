#!/usr/bin/env python3
"""
Load client — Challenge 1.
Connects to all deployed servers, prints per-node load averages and
cluster-wide averages (1, 5, 15 min).

Usage:
  python3 client.py [port]                         # use local machines.txt
  python3 client.py [port] --sync <lab-host>       # fetch fresh machines.txt
                                                   # from NFS first, then run

The --sync flag solves the team stale-list problem: the deploy person uploads
machines.txt to NFS alongside server.py.  Team members run --sync once after
each new deployment to pull the live list before querying the cluster.
"""
import concurrent.futures
import shutil
import socket
import subprocess
import sys

# ── Argument parsing ──────────────────────────────────────────────────────────
args  = sys.argv[1:]
PORT  = 54321
SYNC_HOST: str | None = None

if args and not args[0].startswith('--'):
    PORT = int(args.pop(0))

if '--sync' in args:
    idx = args.index('--sync')
    try:
        SYNC_HOST = args[idx + 1]
    except IndexError:
        print('Error: --sync requires a hostname argument.')
        print('  e.g.  python3 client.py 60012 --sync tp-1a201-08.enst.fr')
        sys.exit(1)

MACHINES_FILE = 'machines.txt'
TIMEOUT       = 5   # seconds per TCP connection attempt

SCP_OPTS = [
    '-4',
    '-o', 'StrictHostKeyChecking=no',
    '-o', 'ConnectTimeout=5',
    '-o', 'BatchMode=yes',
    '-o', 'LogLevel=ERROR',
]


# ── Optional NFS sync ─────────────────────────────────────────────────────────
def sync_machines_from_nfs(host: str) -> None:
    """
    Fetch ~/machines.txt from NFS via SCP.
    deploy.sh uploads machines.txt to NFS alongside server.py, so this
    always reflects the exact list used in the latest deployment.
    """
    print(f'Syncing machines.txt from NFS via {host} ...')
    result = subprocess.run(
        ['scp'] + SCP_OPTS + [f'{host}:~/machines.txt', MACHINES_FILE],
        capture_output=True,
    )
    if result.returncode != 0:
        print(f'  [WARN] Sync failed (host unreachable?). Using local {MACHINES_FILE}.')
    else:
        print(f'  [OK] machines.txt updated from NFS.\n')


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
    if SYNC_HOST:
        sync_machines_from_nfs(SYNC_HOST)

    try:
        with open(MACHINES_FILE) as f:
            machines = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f'Error: {MACHINES_FILE} not found.')
        print('  Deploy first:  ./deploy.sh <port>')
        print('  Or sync:       python3 client.py <port> --sync <lab-host>')
        sys.exit(1)

    total = len(machines)
    print(f'Connecting to {total} machines on port {PORT}...\n')

    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as pool:
        futures = {pool.submit(query, m): m for m in machines}
        raw = [f.result() for f in concurrent.futures.as_completed(futures)]

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