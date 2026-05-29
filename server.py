#!/usr/bin/env python3
"""
Load server (Challenge 1): accepts TCP connections and sends CPU load averages.
Runs on the lab machines. Reads /proc/loadavg.
"""
import socket
import sys

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 54321

def get_load():
    """Reads the 1, 5, and 15 minute load averages."""
    with open('/proc/loadavg') as f:
        parts = f.read().split()
    return float(parts[0]), float(parts[1]), float(parts[2])

def main():
    # Setup dual-stack IPv4/IPv6 listening socket
    srv = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    srv.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
    
    # SO_REUSEADDR allows restarting the server on the same port immediately after killing it
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    srv.bind(('::', PORT))
    srv.listen(64)

    while True:
        conn, _ = srv.accept()
        try:
            l1, l5, l15 = get_load()
            # The basic protocol demanded by the challenge
            conn.sendall(f'{l1} {l5} {l15}\n'.encode())
        except OSError:
            pass
        finally:
            conn.close()

if __name__ == '__main__':
    main()