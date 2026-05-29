#!/usr/bin/env python3
"""
Load client — Challenge 1.
Connects to all deployed servers, prints per-node load averages and
cluster-wide averages (1, 5, 15 min).

Usage: python3 client.py [port]
"""
import concurrent.futures
import socket
import sys

PORT           = int(sys.argv[1]) if len(sys.argv) > 1 else 54321
MACHINES_FILE  = 'machines.txt'
TIMEOUT        = 5          # seconds per connection attempt


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


def main() -> None:
    try:
        with open(MACHINES_FILE) as f:
            machines = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f'Error: {MACHINES_FILE} not found. Run ./deploy.sh first.')
        sys.exit(1)

    total = len(machines)
    print(f'Connecting to {total} machines on port {PORT}...\n')

    results, failed = [], []

    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as pool:
        futures = {pool.submit(query, m): m for m in machines}
        raw = [f.result() for f in concurrent.futures.as_completed(futures)]

    # Sort: reachable nodes first (by hostname), unreachable at the bottom
    raw.sort(key=lambda r: (r[1] is None, r[0]))

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