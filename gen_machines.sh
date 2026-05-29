#!/bin/bash
# Fetch the list of alive tp-1a* machines from the Telecom Paris API
# and write them to machines_alive.txt (only those with 0 connected users).
# Usage: ./gen_machines.sh [output_file]

set -euo pipefail

OUTPUT="${1:-machines_alive.txt}"
API_URL="https://tp.telecom-paris.fr/ajax.php"

curl -s "$API_URL" | python3 -c "
import json, sys
data = json.loads(sys.stdin.read())['data']
machines = [m[0] + '.enst.fr' for m in data if m[1] is True and m[0].startswith('tp-1a') and m[2] == 0 and m[3] == 0 and m[4] == 0]
print('\n'.join(machines))
" > "$OUTPUT"

echo "$(wc -l < "$OUTPUT") machines written to $OUTPUT"
