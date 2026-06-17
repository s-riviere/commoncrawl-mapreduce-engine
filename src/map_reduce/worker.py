#!/usr/bin/env python3
import argparse
import os
import socket
import json
import collections
import subprocess
import shutil
import time
import zlib


def log(level, message):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [WORKER] [{level}] {message}", flush=True)


def clean_local_dir():
    """Nettoie le répertoire local /tmp pour éviter les interférences entre jobs."""
    log("INFO", f"Cleaning local map directory: {LOCAL_MAP_DIR}")
    if os.path.exists(LOCAL_MAP_DIR):
        shutil.rmtree(LOCAL_MAP_DIR)
    os.makedirs(LOCAL_MAP_DIR, exist_ok=True)
    log("INFO", f"Local map directory ready: {LOCAL_MAP_DIR}")

def execute_map(task):
    split_id = task["split_id"]
    n_reducers = task["n_reducers"]
    
    file_name = f"commoncrawl-{split_id:04d}.txt"
    file_path = os.path.join(INPUT_DIR, file_name)
    
    log("INFO", f"MAP start split={split_id} reducers={n_reducers} input={file_path}")
    
    if not os.path.exists(file_path):
        log("ERROR", f"Input split missing: {file_path}")
        return

    os.makedirs(LOCAL_MAP_DIR, exist_ok=True)

    partition_files = {}
    try:
        # 1. On ouvre TOUS les fichiers de partitions en mode "append" dès le départ
        for r_id in range(n_reducers):
            p_path = os.path.join(LOCAL_MAP_DIR, f"partition_{r_id}.txt")
            partition_files[r_id] = open(p_path, "a", encoding="utf-8")

        # 2. Lecture et écriture simultanée au fil de l'eau
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for line_count, line in enumerate(f):
                for word in line.split():
                    if word.isalnum():
                        key = word.lower()
                        h = zlib.crc32(key.encode()) % n_reducers
                        partition_files[h].write(f"{key}\t1\n")

                # Forcer l'écriture sur disque toutes les 50k lignes
                if line_count % 50000 == 0:
                    for f_out in partition_files.values():
                        f_out.flush()

    finally:
        # 3. Fermeture impérative de tous les descripteurs de fichiers (même en cas d'erreur)
        for f_out in partition_files.values():
            f_out.close()

    log("INFO", f"MAP done split={split_id} local_dir={LOCAL_MAP_DIR}")

def execute_reduce(task):
    reducer_id = task["reducer_id"]
    map_workers = task["map_workers"]

    log("INFO", f"REDUCE start reducer={reducer_id} map_workers={len(map_workers)}")
    
    all_values = collections.defaultdict(int)
    ssh_opts = "-o StrictHostKeyChecking=no -o BatchMode=yes -o LogLevel=ERROR"
    
    # SHUFFLE : Récupération des fichiers intermédiaires depuis les /tmp des autres machines
    for worker_ip in map_workers:
        if worker_ip.startswith("::ffff:"):
            worker_ip = worker_ip.replace("::ffff:", "")
        
        remote_partition = os.path.join(LOCAL_MAP_DIR, f"partition_{reducer_id}.txt")
        cmd = f"ssh {ssh_opts} {worker_ip} 'cat {remote_partition}'"
        log("DEBUG", f"REDUCE reducer={reducer_id} fetching partition from {worker_ip}:{remote_partition}")
        
        try:
            # Popen permet de lire le flux de sortie standard en continu
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            # Lecture ligne par ligne depuis le flux réseau SSH
            for line in proc.stdout:
                if not line.strip():
                    continue
                k, v = line.split("\t", 1)
                all_values[k] += int(v)
                
            proc.wait(timeout=5)
            if proc.returncode != 0:
                log("WARN", f"REDUCE reducer={reducer_id} ssh failed host={worker_ip} code={proc.returncode}")
                
        except Exception as e:
            log("ERROR", f"REDUCE reducer={reducer_id} shuffle error host={worker_ip}: {e}")

    final_counts = all_values

    log("DEBUG", f"REDUCE reducer={reducer_id} unique_keys={len(final_counts)}")

    # Publication finale sur le NFS partagé
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file_path = os.path.join(OUTPUT_DIR, f"part-{reducer_id}.txt")
    
    with open(output_file_path, "w", encoding="utf-8") as f:
        for key, total in sorted(final_counts.items(), key=lambda x: x[1], reverse=True):
            f.write(f"{key}\t{total}\n")

    log("INFO", f"REDUCE done reducer={reducer_id} output={output_file_path}")

def main_loop(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((host, port))
        log("INFO", f"Connected to master {host}:{port}")
    except Exception as e:
        log("ERROR", f"Failed to connect to master {host}:{port}: {e}")
        return

    clean_local_dir()
    buffer = ""

    while True:
        # Demande d'une nouvelle tâche
        ready_msg = {"status": "READY_FOR_TASK"}
        log("DEBUG", "Sending READY_FOR_TASK")
        s.sendall((json.dumps(ready_msg) + "\n").encode('utf-8'))
        
        task_line = None
        while True:
            data = s.recv(4096).decode('utf-8')
            if not data:
                log("WARN", "Master connection closed while waiting for task")
                break
            buffer += data
            if "\n" in buffer:
                task_line, buffer = buffer.split("\n", 1)
                break
        
        if not task_line:
            log("WARN", "No task received, stopping worker loop")
            break
            
        task = json.loads(task_line)
        task_type = task.get("type")
        log("INFO", f"Received task type={task_type}")
        
        if task_type == "MAP":
            execute_map(task)
            notification = {"status": "TASK_FINISHED"}
            s.sendall((json.dumps(notification) + "\n").encode('utf-8'))
            log("DEBUG", "Sent TASK_FINISHED for MAP, waiting ACK")
            ack = s.recv(1024)  # Attente de l'ACK du Master
            if ack:
                log("DEBUG", f"Received ACK bytes={len(ack)}")
            else:
                log("WARN", "Master closed connection before ACK after MAP")
                break
            
        elif task_type == "REDUCE":
            execute_reduce(task)
            notification = {"status": "TASK_FINISHED"}
            s.sendall((json.dumps(notification) + "\n").encode('utf-8'))
            log("DEBUG", "Sent TASK_FINISHED for REDUCE, waiting ACK")
            ack = s.recv(1024)  # Attente de l'ACK du Master
            if ack:
                log("DEBUG", f"Received ACK bytes={len(ack)}")
            else:
                log("WARN", "Master closed connection before ACK after REDUCE")
                break
            
        elif task_type == "WAIT":
            log("DEBUG", "Received WAIT from master, sleeping 2s")
            time.sleep(2)
            buffer = ""  # Clear buffer to avoid processing stale data
            
        elif task_type == "SHUTDOWN":
            log("INFO", "Received SHUTDOWN from master")
            break

        else:
            log("WARN", f"Unknown task type received: {task_type}")

    s.close()
    log("INFO", "Worker process exiting")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MapReduce Worker", add_help=False)
    parser.add_argument("-h", "--host", required=True, metavar="HOST", help="Adresse IP ou hostname du Master")
    parser.add_argument("-p", "--port", required=True, type=int, metavar="PORT", help="Port d'écoute du Master")
    parser.add_argument("-i", "--input-dir", required=True, metavar="DIR", help="Dossier d'entrée des splits")
    parser.add_argument("-o", "--output-dir", required=True, metavar="DIR", help="Dossier de sortie des reducers")
    parser.add_argument("-l", "--local-map-dir", required=True, metavar="DIR", help="Dossier local des sorties intermédiaires Map")
    parser.add_argument("--help", action="help", help="Afficher ce message d'aide et quitter")
    args = parser.parse_args()

    INPUT_DIR = os.path.expanduser(args.input_dir)
    OUTPUT_DIR = os.path.expanduser(args.output_dir)
    LOCAL_MAP_DIR = os.path.expanduser(args.local_map_dir)

    main_loop(args.host, args.port)
