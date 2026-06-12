#!/usr/bin/env python3
import socket
import json
import sys
import signal
import importlib.util
import os

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 54321

def load_user_function(module_name: str, func_name: str):
    """Charge dynamiquement une fonction depuis un fichier python local."""
    file_path = f"{module_name}.py"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Le module utilisateur {file_path} est introuvable sur le worker.")
        
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, func_name)

def handle_client(conn):
    buffer = b""
    try:
        while True:
            chunk = conn.recv(4096)
            if not chunk:  
                break
            buffer += chunk
            if b"\n" in chunk:
                break
        
        if not buffer:
            return

        request = json.loads(buffer.decode('utf-8').strip())
        task_type = request.get("task")
        data = request.get("data")
        job_name = request.get("job_name") # Nom du fichier contenant les fonctions (ex: "wordcount")
        
        if task_type == "MAP":
            # Chargement dynamique de la fonction mapper de l'utilisateur
            user_map = load_user_function(job_name, "mapper")
            result = user_map(data)
            response = {"status": "OK", "result": result}
            
        elif task_type == "REDUCE":
            # Chargement dynamique de la fonction reducer de l'utilisateur
            user_reduce = load_user_function(job_name, "reducer")
            result = user_reduce(data["key"], data["values"])
            response = {"status": "OK", "result": result}
            
        else:
            response = {"status": "ERROR", "message": "Unknown task"}
            
        conn.sendall(json.dumps(response).encode('utf-8'))
    except Exception as e:
        error_resp = {"status": "ERROR", "message": str(e)}
        try:
            conn.sendall(json.dumps(error_resp).encode('utf-8'))
        except OSError:
            pass

def main():
    srv = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    srv.setsockopt(socket.IPPROTO_IPC6, socket.IPV6_V6ONLY, 0)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('::', PORT))
    srv.listen(64)
    
    def _shutdown(signum, frame):
        srv.close()
        sys.exit(0)
        
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)
    
    print(f"Worker générique en écoute sur le port {PORT}...")
    while True:
        try:
            conn, _ = srv.accept()
            handle_client(conn)
            conn.close()
        except OSError:
            pass

if __name__ == '__main__':
    main()