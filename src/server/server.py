#!/usr/bin/env python3
"""
Load server — Challenge 1.
Accepts TCP connections and replies with the 1/5/15-min CPU load averages.
Runs on lab machines.  Reads /proc/loadavg.

Usage: python3 server.py [port]
"""
import signal
import socket
import sys

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 54321

def get_load() -> tuple[float, float, float]:
    """Return (load1, load5, load15) from /proc/loadavg."""
    with open('/proc/loadavg') as f:
        parts = f.read().split()
    return float(parts[0]), float(parts[1]), float(parts[2])

def main() -> None:
    # Dual-stack IPv4/IPv6 listening socket
    srv = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    srv.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)

    # Allow immediate restart on the same port after a kill
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    srv.bind(('::', PORT))
    srv.listen(64)

    # Clean shutdown on SIGTERM / SIGINT so the port is released quickly
    def _shutdown(signum, frame):
        srv.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    while True:
        conn, _ = srv.accept()
        try:
            l1, l5, l15 = get_load()
            conn.sendall(f'{l1} {l5} {l15}\n'.encode())
        except OSError:
            pass
        finally:
            conn.close()

if __name__ == '__main__':
    main()
