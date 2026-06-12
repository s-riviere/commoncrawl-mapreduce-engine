#!/usr/bin/env python3
"""
master.py — Orchestrateur MapReduce générique.
"""
import socket
import json
import sys
import cluster

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

MACHINES_FILE = '/tmp/slr207-group1/machines.txt'
TIMEOUT = 5

def send_task(host: str, port: int, task_type: str, job_name: str, data: dict | str) -> dict | None:
    """Envoie une tâche réseau incluant le nom du job à exécuter."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(TIMEOUT)
            s.connect((host, port))
            
            payload = json.dumps({"task": task_type, "job_name": job_name, "data": data})
            s.sendall(payload.encode('utf-8') + b'\n')
            
            chunks = []
            while True:
                chunk = s.recv(4048)
                if not chunk:
                    break
                chunks.append(chunk)
                
            return json.loads(b''.join(chunks).decode('utf-8'))
    except Exception:
        return None

def main() -> None:
    # Récupération de tous les paramètres via le module cluster
    job_name, port, sync_host = cluster.parse_arguments(sys.argv[1:])
    
    machines = cluster.load_machines(sync_host, MACHINES_FILE)
    total_workers = len(machines)
    
    if total_workers == 0:
        print("Erreur : Aucun worker disponible.")
        sys.exit(1)

    print(f"Démarrage du Job '{job_name}' sur le port {port} avec {total_workers} workers.\n")

    input_splits = [
        "hello world",
        "map reduce python",
        "hello map reduce",
        "distributed systems systems"
    ]

    # 1. PHASE MAP
    print("--- Lancement de la Phase MAP ---")
    map_results = []
    for i, text in enumerate(input_splits):
        worker = machines[i % total_workers]
        res = send_task(worker, port, "MAP", job_name, text)
        if res and res.get("status") == "OK":
            map_results.extend(res["result"])
        elif res and res.get("status") == "ERROR":
            print(f"  Erreur Worker: {res.get('message')}")

    # 2. PHASE SHUFFLE
    print("\n--- Lancement de la Phase SHUFFLE ---")
    shuffled_data = {}
    for key, value in map_results:
        if key not in shuffled_data:
            shuffled_data[key] = []
        shuffled_data[key].append(value)

    # 3. PHASE REDUCE
    print("\n--- Lancement de la Phase REDUCE ---")
    final_results = {}
    for i, (key, values) in enumerate(shuffled_data.items()):
        worker = machines[i % total_workers]
        res = send_task(worker, port, "REDUCE", job_name, {"key": key, "values": values})
        if res and res.get("status") == "OK":
            r_key, r_val = res["result"]
            final_results[r_key] = r_val

    # 4. AFFICHAGE DES RÉSULTATS
    print(f'\n{"=" * 55}\n RÉSULTATS DU JOB : {job_name}\n{"=" * 55}')
    for k, v in sorted(final_results.items()):
        print(f'  {k:<20} : {v}')
    print('=' * 55)

if __name__ == '__main__':
    main()