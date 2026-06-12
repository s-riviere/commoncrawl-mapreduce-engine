#!/usr/bin/env python3
"""
cluster.py — Gestion des machines, des arguments et de la synchronisation.
"""
import sys
import subprocess

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

def sync_machines(host: str, filename: str) -> None:
    """Récupère le fichier des machines depuis l'hôte distant via SCP."""
    remote_path = f'{host}:/tmp/slr207-group1/{filename}'
    print(f'[SYNC] Fetching {filename} from {remote_path} ...')

    result = subprocess.run(
        ['scp'] + SCP_OPTS + [remote_path, filename],
        capture_output=True,
    )
    if result.returncode == 0:
        print(f'[SYNC] OK — got {filename} from {host}\n')
    else:
        print(f'[SYNC] Failed to fetch {filename}. Proceeding with local file.\n')

def load_machines(sync_host: str | None, filename: str) -> list[str]:
    """Lit le fichier et retourne la liste des adresses des machines."""
    if sync_host:
        sync_machines(sync_host, filename)

    try:
        with open(filename) as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f'Error: {filename} not found.')
        sys.exit(1)