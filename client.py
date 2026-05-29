#!/usr/bin/env python3
"""
Load client — Challenge 1.
Connects to all deployed servers, prints per-node load averages and
cluster-wide averages (1, 5, 15 min).

Usage:
  python3 client.py [port]               use local machines.txt
  python3 client.py [port] --sync        try every host in local machines.txt
                                         until one serves ~/machines.txt via NFS
  python3 client.py [port] --sync HOST   try HOST first, then fall back as above
"""
import concurrent.futures
import socket
import subprocess
import sys

# ── Argument parsing ──────────────────────────────────────────────────────────
args = sys.argv[1:]
PORT = 54321
DO_SYNC   = False
SYNC_HINT = None          # optional preferred host supplied by the deploy person

if args and not args[0].startswith('--'):
    PORT = int(args.pop(0))

if '--sync' in args:
    DO_SYNC = True
    idx = args.index('--sync')
    # --sync HOST  →  try HOST first
    # --sync       →  no hint, iterate machines.txt directly
    if idx + 1 < len(args) and not args[idx + 1].startswith('--'):
        SYNC_HINT = args[idx + 1]

MACHINES_FILE = 'machines.txt'
TIMEOUT       = 5   # seconds per TCP connection attempt

SCP_OPTS = [
    '-4',
    '-o', 'StrictHostKeyChecking=no',
    '-o', 'ConnectTimeout=4',
    '-o', 'BatchMode=yes',
    '-o', 'LogLevel=ERROR',
]


# ── NFS sync ──────────────────────────────────────────────────────────────────
def sync_machines_from_nfs() -> None:
    """
    Fetch machines.txt from /tmp on a lab machine.

    Strategy:
      1. If a SYNC_HINT host was given (printed by deploy.sh), try it first.
      2. Fall back to every host in the local machines.txt (which may be stale).
      3. If nothing works, warn and proceed with whatever is on disk.
    """
    remote_dir = '/tmp/slr207-group1'

    # Build candidate list: hint first, then whatever is in local file
    candidates: list[str] = []
    if SYNC_HINT:
        candidates.append(SYNC_HINT)

    try:
        with open(MACHINES_FILE) as f:
            for line in f:
                h = line.strip()
                if h and h not in candidates:
                    candidates.append(h)
    except FileNotFoundError:
        pass  # No local file at all — we can only try the hint

    if not candidates:
        print('[SYNC] No hosts to try (no hint given and no local machines.txt).')
        print('       Ask the deploy person for a --sync <host> argument.')
        return

    print(f'[SYNC] Fetching machines.txt from /tmp on lab machine '
          f'(trying up to {len(candidates)} host(s))...')

    for host in candidates:
        result = subprocess.run(
            ['scp'] + SCP_OPTS + [f'{host}:{remote_dir}/machines.txt', MACHINES_FILE],
            capture_output=True,
        )
        if result.returncode == 0:
            print(f'[SYNC] OK — got machines.txt via {host}\n')
            return
        # else: try next host silently

    # Nothing worked
    print('[SYNC] Could not reach any lab machine via SCP.')
    print('       Possible causes:')
    print('         • SSH key not set up: run ssh-copy-id once to any lab machine')
    print('         • Not on campus Wi-Fi (eduroam is a different network)')
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
        sync_machines_from_nfs()

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