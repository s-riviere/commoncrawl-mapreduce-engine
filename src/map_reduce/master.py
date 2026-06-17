#!/usr/bin/env python3
import socket
import threading
import json
import argparse


def normalize_worker_host(host):
    """Convert IPv4-mapped IPv6 addresses (::ffff:a.b.c.d) to plain IPv4."""
    if host.startswith("::ffff:"):
        return host.replace("::ffff:", "", 1)
    return host

class MasterServer:
    def __init__(self, port, n_splits, n_reducers):
        self.port = port
        self.n_splits = n_splits
        self.n_reducers = n_reducers
        self.lock = threading.Lock()
        self.map_tasks = []
        self.reduce_tasks = []
        self.phase = "MAP"  # "MAP", "BARRIER", "REDUCE"
        self.completed_tasks = 0
        self.total_map_tasks = 0
        self.total_reduce_tasks = 0
        self.active_map_workers = set()
        self.active_connections = 0
        self.job_completed = False

    def load_tasks(self):
        # Le master envoie uniquement l'identifiant numérique du split
        self.map_tasks = [{"type": "MAP", "split_id": i, "n_reducers": self.n_reducers} for i in range(self.n_splits)]
        self.total_map_tasks = len(self.map_tasks)
        
        self.reduce_tasks = [{"type": "REDUCE", "reducer_id": i} for i in range(self.n_reducers)]
        self.total_reduce_tasks = len(self.reduce_tasks)
        print(f"[MASTER] Tâches chargées : {self.total_map_tasks} MAPs, {self.total_reduce_tasks} REDUCEs.")

    def handle_worker(self, conn, addr):
        worker_host = normalize_worker_host(addr[0])
        print(f"[MASTER] Connexion établie avec le worker : {worker_host}")

        with self.lock:
            self.active_connections += 1
        
        # Buffer pour accumuler les fragments de texte jusqu'au saut de ligne
        buffer = ""
        
        while True:
            try:
                data = conn.recv(4096).decode('utf-8')
                if not data:
                    break
                
                buffer += data
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if not line.strip():
                        continue
                    
                    message = json.loads(line)
                    status = message.get("status")
                    
                    if status == "READY_FOR_TASK":
                        with self.lock:
                            if self.phase == "MAP":
                                if self.map_tasks:
                                    task = self.map_tasks.pop(0)
                                    print(f"[MASTER] {worker_host} starts MAP {task['split_id']}")
                                    conn.sendall((json.dumps(task) + "\n").encode('utf-8'))
                                else:
                                    # Entrée dans la barrière de synchronisation active
                                    wait_task = {"type": "WAIT"}
                                    conn.sendall((json.dumps(wait_task) + "\n").encode('utf-8'))
                                    
                            elif self.phase == "REDUCE":
                                if self.reduce_tasks:
                                    task = self.reduce_tasks.pop(0)
                                    # Injection dynamique de la liste des workers ayant produit des maps
                                    task["map_workers"] = list(self.active_map_workers)
                                    print(f"[MASTER] {worker_host} starts REDUCE {task['reducer_id']}")
                                    conn.sendall((json.dumps(task) + "\n").encode('utf-8'))
                                else:
                                    shutdown_task = {"type": "SHUTDOWN"}
                                    conn.sendall((json.dumps(shutdown_task) + "\n").encode('utf-8'))
                                    return
                                    
                    elif status == "TASK_FINISHED":
                        with self.lock:
                            print(f"[MASTER] {worker_host} finished a task")
                            self.completed_tasks += 1
                            # Enregistrement du worker pour le shuffle futur
                            self.active_map_workers.add(worker_host)
                            
                            # Envoi de l'acquittement (ACK)
                            ack = {"status": "ACK"}
                            conn.sendall((json.dumps(ack) + "\n").encode('utf-8'))
                            
                            # Vérification des changements de phase
                            if self.phase == "MAP" and self.completed_tasks == self.total_map_tasks:
                                print("[MASTER] Phase MAP terminée. Passage à la phase REDUCE.")
                                self.phase = "REDUCE"
                                self.completed_tasks = 0
                            elif self.phase == "REDUCE" and self.completed_tasks == self.total_reduce_tasks:
                                print("[MASTER] Phase REDUCE terminée. Fin du job.")
                                self.job_completed = True
                                
            except Exception as e:
                print(f"[MASTER] Erreur avec le worker {worker_host} : {e}")
                break
                
        conn.close()
        with self.lock:
            self.active_connections -= 1
        print(f"[MASTER] Connexion fermée avec le worker : {worker_host}")

    def run(self):
        self.load_tasks()
        server = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        server.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("::", self.port))
        server.listen(64)
        server.settimeout(1)
        print(f"[MASTER] Serveur en écoute (Dual-stack) sur le port {self.port} (Reducers: {self.n_reducers})...")
        
        try:
            while True:
                with self.lock:
                    if self.job_completed:
                        break

                try:
                    conn, addr = server.accept()
                except socket.timeout:
                    continue

                t = threading.Thread(target=self.handle_worker, args=(conn, addr))
                t.daemon = True
                t.start()
        except KeyboardInterrupt:
            pass
        finally:
            print("[MASTER] Arrêt du serveur.")
            server.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Master Server pour MapReduce.")
    parser.add_argument("-p", "--port", type=int, default=54321, help="Port d'écoute (par défaut: 54321)")
    parser.add_argument("-s", "--splits", type=int, default=10, help="Nombre de splits MAP à traiter (par défaut: 10)")
    parser.add_argument("-r", "--reducers", type=int, default=10, help="Nombre de reducers (par défaut: 10)")
    args = parser.parse_args()

    if args.splits <= 0:
        parser.error("--splits doit être strictement positif")

    master = MasterServer(port=args.port, n_reducers=args.reducers, n_splits=args.splits)
    master.run()
