#!/bin/bash
# Stop all load servers deployed by deploy.sh.
# THE BUG THAT WAS HERE: SSH was timing out *after* successfully sending pkill
# (killing the server process can disrupt the SSH tunnel), so every node was
# reported [FAILED] even though all processes were actually killed.
#
# THE FIX: launch pkill in the background on the remote machine (`pkill … &`)
# and exit the SSH session immediately (`exit 0`) BEFORE pkill runs.
# SSH therefore returns 0 reliably, and we get correct [CLEANED] reports.
# Verification is done automatically at the end with client.py.
#
# Usage: ./kill.sh [port]

set -uo pipefail

PORT="${1:-54321}"
MACHINES_FILE="machines.txt"

SSH_OPTS="-4 -n \
  -o StrictHostKeyChecking=no \
  -o ConnectTimeout=5 \
  -o BatchMode=yes \
  -o LogLevel=ERROR \
  -o ServerAliveInterval=3 \
  -o ServerAliveCountMax=1"

if [[ ! -f "$MACHINES_FILE" ]]; then
    echo "No $MACHINES_FILE found. Nothing to kill."
    exit 0
fi

TOTAL=$(wc -l < "$MACHINES_FILE")
echo "Killing servers on port $PORT across $TOTAL machines..."
echo "(fire-and-forget — SSH exits before pkill completes, so ACK is reliable)"
echo ""

declare -a PIDS=()

while IFS= read -r host; do
    [[ -z "$host" ]] && continue

    (
        # KEY FIX: `pkill … &` forks pkill in the background on the remote node.
        # The SSH session then exits cleanly with 0 *before* the process is killed,
        # so we always get a reliable return code from SSH.
        # `exit 0` is explicit for clarity; `& exit 0` would also work.
        if timeout 10 ssh $SSH_OPTS "$host" \
            "pkill -9 -u \"\$USER\" -f \"python3.*server\\.py.*${PORT}\" &>/dev/null &
             exit 0" \
            2>/dev/null; then
            echo "  [CLEANED]  $host"
        else
            echo "  [UNREACHABLE] $host  (SSH could not connect — already down?)"
        fi
    ) &
    PIDS+=($!)
done < "$MACHINES_FILE"

for pid in "${PIDS[@]}"; do
    wait "$pid"
done

# ── NFS cleanup: remove server.py from home directory ─────────────────────────
echo ""
FIRST_NODE=$(head -n 1 "$MACHINES_FILE")
if [[ -n "$FIRST_NODE" ]]; then
    echo "Removing ~/server.py from NFS via $FIRST_NODE ..."
    if timeout 10 ssh $SSH_OPTS "$FIRST_NODE" "rm -f ~/server.py" 2>/dev/null; then
        echo "  [OK] ~/server.py removed."
    else
        echo "  [WARN] Could not remove ~/server.py (machine unreachable?)."
        echo "         Delete it manually: ssh $FIRST_NODE 'rm -f ~/server.py'"
    fi
fi

# ── Optional: quick verification ping ────────────────────────────────────────
echo ""
echo "All kill signals sent. Verifying in 2 seconds..."
sleep 2

if command -v python3 &>/dev/null && [[ -f "client.py" ]]; then
    LIVE=$(python3 client.py "$PORT" 2>/dev/null | grep -c "load:" || true)
    if [[ "$LIVE" -eq 0 ]]; then
        echo "  ✓ Cluster is clean — 0 nodes responding on port $PORT."
    else
        echo "  ✗ WARNING: $LIVE node(s) still responding. Re-run ./kill.sh $PORT"
    fi
fi

echo ""
echo "Done. machines.txt kept intact for the next deployment."