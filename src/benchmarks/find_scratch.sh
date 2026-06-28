#!/usr/bin/env bash
# ==============================================================================
# find_scratch.sh — §2 [Advanced]: explore partitions beyond /tmp for spilling.
#
# The default /tmp can be a small tmpfs (RAM-backed) on the lab machines, which
# saturates when hundreds of MAP downloads land at once. This script inspects
# the mounted filesystems (df/mount), keeps only LOCAL, WRITABLE partitions that
# are NOT NFS (the brief forbids NFS for bulk data), and recommends the largest
# one as a scratch/spill directory for the worker's --spill-dir / -l flags.
#
# Usage:
#   bash src/benchmarks/find_scratch.sh                 # inspect THIS machine
#   ssh <host> 'bash -s' < src/benchmarks/find_scratch.sh   # inspect a cluster node
# ==============================================================================
set -euo pipefail

echo "=== Host: $(hostname) ==="
echo

echo "--- /tmp backing store ---"
# Is /tmp a RAM-backed tmpfs (small, volatile) or real disk?
tmp_fstype="$(stat -f -c %T /tmp 2>/dev/null || echo unknown)"
df -h /tmp 2>/dev/null | sed 's/^/  /'
echo "  /tmp filesystem type: ${tmp_fstype}"
echo

echo "--- Candidate local writable partitions (NFS excluded) ---"
# Columns: Filesystem Type Size Used Avail Use% Mounted-on
# Exclude network/pseudo filesystems; keep only mounts we can actually write to.
best_dir=""
best_avail_kb=0

while read -r fs fstype size used avail usep mount; do
    case "${fstype}" in
        nfs|nfs4|cifs|smb|smbfs|fuse.sshfs|autofs|overlay|squashfs|proc|sysfs|devtmpfs|cgroup*|tracefs|debugfs)
            continue ;;
    esac
    # Need write access to use it as scratch.
    [[ -w "${mount}" ]] || continue

    # Available space in KB for comparison (df -k --output gives KB).
    avail_kb="$(df -k --output=avail "${mount}" 2>/dev/null | tail -1 | tr -d ' ')"
    [[ "${avail_kb}" =~ ^[0-9]+$ ]] || continue

    printf "  %-28s %-8s avail=%-8s use=%-5s %s\n" "${fs}" "${fstype}" "${avail}" "${usep}" "${mount}"

    if (( avail_kb > best_avail_kb )); then
        best_avail_kb="${avail_kb}"
        best_dir="${mount}"
    fi
done < <(df -P -T -k 2>/dev/null | tail -n +2)

echo
if [[ -z "${best_dir}" ]]; then
    echo "No suitable local scratch partition found; fall back to /tmp."
    exit 0
fi

# Prefer a per-user subdirectory so several users don't collide on the mount.
scratch="${best_dir%/}/scratch-${USER:-$(id -un)}"
echo "=== Recommendation ==="
echo "  Largest local writable mount : ${best_dir} ($(( best_avail_kb / 1024 / 1024 )) GiB free)"
echo "  Suggested scratch directory  : ${scratch}"
echo
echo "  Use it with the worker, e.g.:"
echo "    mkdir -p '${scratch}'"
echo "    python3 src/mapreduce/worker.py ... \\"
echo "        --spill-dir '${scratch}' -l '${scratch}/map-outputs'"
