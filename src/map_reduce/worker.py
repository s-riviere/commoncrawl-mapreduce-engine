#!/usr/bin/env python3
import socket
import json
import sys
import signal
import importlib

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 54321

def load_user_function(module_name: str, func_name: str):
    try:
        module = importlib.import_module(module_name)
        return getattr(module, func_name)
    except ModuleNotFoundError:
        raise FileNotFoundError(f"Le module utilisateur '{module_name}.py' est introuvable au même niveau.")

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
        job_name = request.get("job_name")
        
        if task_type == "MAP":
            user_map = load_user_function(job_name, "mapper")
            result = user_map(data)
            response = {"status": "OK", "result": result}
            
        elif task_type == "REDUCE":
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
    srv.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
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
