#!/usr/bin/env python3
"""
Load client: connects to all alive servers via Campus Wi-Fi, prints per-node loads 
and the cluster-wide averages (1, 5, 15 min).
Usage: python3 client.py [port]
"""
import concurrent.futures
import socket
import sys

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 54321
# PLUS DE MACHINES_ALIVE ! On utilise directement le fichier officiel
MACHINES_FILE = 'machines.txt'
TIMEOUT = 5

def query(host):
    """Connects to a single host via TCP and retrieves its load average."""
    try:
        # Try resolving addresses over the local subnet
        for af, socktype, proto, _, sa in socket.getaddrinfo(host, PORT, type=socket.SOCK_STREAM):
            try:
                with socket.socket(af, socktype, proto) as s:
                    s.settimeout(TIMEOUT)
                    s.connect(sa)
                    chunks = []
                    while True:
                        b = s.recv(1024)
                        if not b:
                            break
                        chunks.append(b)
                data = b''.join(chunks).decode().strip()
                l1, l5, l15 = map(float, data.split())
                return host, l1, l5, l15
            except OSError:
                continue
        return host, None, None, None
    except Exception:
        return host, None, None, None

def main():
    try:
        with open(MACHINES_FILE) as f:
            machines = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f'Error: {MACHINES_FILE} not found. Run ./deploy.sh first.')
        sys.exit(1)

    print(f'Connecting to {len(machines)} machines on port {PORT}...\n')
    results, failed = [], []

    # Parallelize network requests for speed
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as ex:
        for fut in concurrent.futures.as_completed({ex.submit(query, m): m for m in machines}):
            host, l1, l5, l15 = fut.result()
            if l1 is None:
                failed.append(host)
                print(f'  {host:<35} UNREACHABLE')
            else:
                results.append((host, l1, l5, l15))
                print(f'  {host:<35} load: {l1:.2f}  {l5:.2f}  {l15:.2f}')

    n = len(results)
    print(f'\n{"=" * 55}')
    print(f'  Nodes responded: {n}/{len(machines)}')
    
    if n > 0:
        a1 = sum(r[1] for r in results) / n
        a5 = sum(r[2] for r in results) / n
        a15 = sum(r[3] for r in results) / n
        print(f'  Avg cluster load  1 min : {a1:.4f}')
        print(f'  Avg cluster load  5 min : {a5:.4f}')
        print(f'  Avg cluster load 15 min : {a15:.4f}')
    print('=' * 55)
    
    if failed:
        print(f'\n  Unreachable ({len(failed)}): {", ".join(failed)}')

if __name__ == '__main__':
    main()