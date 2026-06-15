#!/usr/bin/env python3
"""
cluster.py — Gestion des machines, des arguments et de la synchronisation.
"""
import sys
import subprocess
from pathlib import Path

LOCAL_MACHINES_FILE = str(Path(__file__).resolve().parent.parent.parent / "runtime" / "machines.txt")
REMOTE_MACHINES_FILE = '/tmp/slr207-group1-bis/machines.txt'
SCP_OPTS = [
    '-4',
    '-o', 'StrictHostKeyChecking=no',
    '-o', 'ConnectTimeout=10',
    '-o', 'BatchMode=yes',
    '-o', 'LogLevel=ERROR',
]

def parse_arguments(args: list) -> tuple[str, int, str | None]:
    """Analyse les arguments et retourne (job_name, port, sync_host)."""
    if not args or args[0].startswith('--'):
        print("Error: You must specify the job name as the first argument.")
        print("Usage: python3 master.py <job_name> [port] [--sync HOST]")
        sys.exit(1)

    # Extraction du premier argument obligatoire : le nom du job
    job_name = args.pop(0)
    
    port = 54321
    sync_host = None

    if args and not args[0].startswith('--'):
        port = int(args.pop(0))

    if '--sync' in args:
        idx = args.index('--sync')
        if idx + 1 < len(args) and not args[idx + 1].startswith('--'):
            sync_host = args[idx + 1]
        else:
            print('Error: --sync requires a HOST argument.')
            sys.exit(1)
            
    return job_name, port, sync_host

def sync_machines(host: str) -> None:
    """Récupère le fichier des machines depuis l'hôte distant via SCP."""
    print(f"[SYNC] Fetching from {REMOTE_MACHINES_FILE} ... ", end="", flush=True)

    result = subprocess.run(
        ['scp'] + SCP_OPTS + [f'{host}:{REMOTE_MACHINES_FILE}', LOCAL_MACHINES_FILE],
        capture_output=True,
    ).returncode

    if result == 0:
        print('[OK]')
    else:
        print('[FAILED]')
        print("    Proceeding with local machines.txt (may be stale).\n")
    print("")

def load_machines(sync_host: str | None) -> list[str]:
    """Lit le fichier et retourne la liste des adresses des machines."""
    if sync_host:
        sync_machines(sync_host)

    try:
        with open(LOCAL_MACHINES_FILE) as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f'Error: {LOCAL_MACHINES_FILE} not found.')
        sys.exit(1)
