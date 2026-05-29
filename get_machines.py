#!/usr/bin/env python3
"""
Queries the Telecom Paris live room tracker API.
Parses the JSON directly to filter for ALIVE and FREE machines,
sorting them to prioritize machines with 0 users.
"""
import urllib.request
import json
import sys

API_URL = "https://tp.telecom-paris.fr/ajax.php" 

def main():
    print(f"Querying school API: {API_URL}")
    try:
        req = urllib.request.Request(API_URL, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as response:
            raw_data = response.read().decode('utf-8')
            json_data = json.loads(raw_data)
    except Exception as e:
        print(f"Error fetching from API: {e}")
        print("Make sure you are actively connected to the Telecom Campus Wi-Fi!")
        sys.exit(1)

    machines_data = json_data.get("data", [])
    
    valid_machines = []
    
    for item in machines_data:
        if len(item) >= 5:
            name = item[0]
            is_alive = item[1]
            
            if is_alive is True:
                try:
                    occupation_score = int(item[2]) + int(item[3]) + int(item[4])
                except ValueError:
                    occupation_score = 999
                
                valid_machines.append((name, occupation_score))

    valid_machines.sort(key=lambda x: x[1])

    best_machines = valid_machines[:50]

    if not best_machines:
        print("Error: No active machines found in JSON data.")
        sys.exit(1)

    with open("machines.txt", "w") as f:
        for machine_name, score in best_machines:
            f.write(f"{machine_name}.enst.fr\n")

    print(f"Successfully tracked and saved {len(best_machines)} ALIVE and FREE machines to machines.txt")

if __name__ == "__main__":
    main()