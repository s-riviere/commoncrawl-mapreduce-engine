#!/usr/bin/env python3
'''
Load server: accepts TCP connections and sends CPU load averages (1/5/15 min).
Writes its PID to ~/.server.pid for clean shutdown by kill.sh.
On startup, kills any previous instance whose PID is recorded in the file.
Usage: python3 server.py [port]
'''
import atexit
import os
import signal
import socket
import sys
import time

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 54321
PID_FILE = os.path.expanduser('~/.server.pid')


def cleanup():
    try:
        # Only remove the file if it still contains OUR pid
        with open(PID_FILE) as f:
            if int(f.read().strip()) == os.getpid():
                os.remove(PID_FILE)
    except (OSError, ValueError):
        pass


def kill_predecessor():
    '''Kill any previous server instance whose PID is in the file.'''
    try:
        with open(PID_FILE) as f:
            old_pid = int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return
    if old_pid == os.getpid():
        return
    try:
        os.kill(old_pid, signal.SIGTERM)
        # Wait briefly for it to exit and release the port
        for _ in range(20):
            time.sleep(0.1)
            try:
                os.kill(old_pid, 0)   # check still alive
            except ProcessLookupError:
                break
        else:
            # Still alive -> force kill
            try:
                os.kill(old_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    except (ProcessLookupError, PermissionError):
        pass


def get_load():
    with open('/proc/loadavg') as f:
        parts = f.read().split()
    return float(parts[0]), float(parts[1]), float(parts[2])


def main():
    kill_predecessor()

    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()) + '\n')
    atexit.register(cleanup)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    srv = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    srv.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('::', PORT))
    srv.listen(64)

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

