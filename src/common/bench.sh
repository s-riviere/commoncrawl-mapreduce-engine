#!/usr/bin/env bash
# bench.sh — lightweight phase timing for the shell scripts (deploy / cleanup).
#
# This is the shell-side companion to bench.py. It can't do statistical
# micro-benchmarks (shell phases run once), but it provides the same model:
# named phases, nanosecond resolution, a ranked report and a flagged
# bottleneck, all measured against the wall-clock of the whole run.
#
# Usage:
#   source "path/to/bench.sh"
#   bench_init
#   bench_start fetch_machines
#   ... work ...
#   bench_end   fetch_machines
#   bench_wrap  upload  scp ...        # time a single command
#   bench_report

declare -gA _BENCH_NS      # phase -> accumulated nanoseconds
declare -gA _BENCH_T0      # phase -> last start timestamp (ns)
declare -ga _BENCH_ORDER   # phases in first-seen order
_BENCH_WALL0=0

# Nanosecond timestamp. GNU date (Linux lab machines) supports %N.
_bench_now() { date +%s%N; }

bench_init() {
    _BENCH_NS=()
    _BENCH_T0=()
    _BENCH_ORDER=()
    _BENCH_WALL0=$(_bench_now)
}

bench_start() {
    local p="$1"
    if [[ -z "${_BENCH_NS[$p]+set}" ]]; then
        _BENCH_NS["$p"]=0
        _BENCH_ORDER+=("$p")
    fi
    _BENCH_T0["$p"]=$(_bench_now)
}

bench_end() {
    local p="$1"
    local now t0
    now=$(_bench_now)
    t0="${_BENCH_T0[$p]:-$now}"
    _BENCH_NS["$p"]=$(( ${_BENCH_NS[$p]:-0} + now - t0 ))
}

# bench_wrap <phase> <command...> : time a single command, preserve its exit code.
bench_wrap() {
    local p="$1"; shift
    bench_start "$p"
    "$@"
    local rc=$?
    bench_end "$p"
    return $rc
}

_bench_fmt() {  # nanoseconds -> human readable
    local ns="$1"
    if   (( ns >= 1000000000 )); then awk -v n="$ns" 'BEGIN{printf "%.3f s",  n/1e9}'
    elif (( ns >= 1000000    )); then awk -v n="$ns" 'BEGIN{printf "%.3f ms", n/1e6}'
    elif (( ns >= 1000       )); then awk -v n="$ns" 'BEGIN{printf "%.3f us", n/1e3}'
    else echo "${ns} ns"; fi
}

bench_report() {
    local wall now p ns pct max=0 maxp=""
    now=$(_bench_now)
    wall=$(( now - _BENCH_WALL0 ))
    (( wall == 0 )) && wall=1

    echo ""
    echo "================== TIMING REPORT =================="
    printf "  %-24s %12s %8s\n" "phase" "time" "%wall"
    echo "  -----------------------------------------------"
    for p in "${_BENCH_ORDER[@]}"; do
        ns="${_BENCH_NS[$p]}"
        pct=$(awk -v a="$ns" -v w="$wall" 'BEGIN{printf "%.1f", a*100.0/w}')
        printf "  %-24s %12s %7s%%\n" "$p" "$(_bench_fmt "$ns")" "$pct"
        if (( ns > max )); then max="$ns"; maxp="$p"; fi
    done
    echo "  -----------------------------------------------"
    printf "  %-24s %12s\n" "wall clock" "$(_bench_fmt "$wall")"
    if [[ -n "$maxp" ]]; then
        echo "  -----------------------------------------------"
        echo "  Bottleneck: $maxp ($(_bench_fmt "$max"))"
    fi
    echo "==================================================="
}
