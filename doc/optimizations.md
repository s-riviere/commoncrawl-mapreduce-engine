# Worker Optimization Log

Baseline: `main` branch, Python worker, 32 splits, 8 reducers, cluster of lab machines.
All measurements aggregated across workers (summed t_compute, t_shuffle etc.).

---

## Baseline — Python worker (unoptimized)

**Branch:** `main`  
**File:** `src/mapreduce/worker.py`

### MAP phase (per run, summed across workers)
- Tokenization: `line.split()` + `word.isalnum()` check per word
- CRC32 called **once per word occurrence** (`zlib.crc32(key.encode()) % n_reducers`)
- File read: `readlines()` as UTF-8 text
- Write: one `word\t1\n` line per occurrence (no pre-aggregation)

### REDUCE phase
- SSH shuffle: **sequential** — one `ssh cat` per map worker, waited serially
- `t_shuffle` accumulates across all N workers

### Benchmark results (`amdahl_results_python.json`, 32 splits)

| N  | t_total (s) | t_compute (s) | t_shuffle (s) | Speedup |
|----|-------------|---------------|---------------|---------|
| 1  | 275.4       | 234.9         | 20.9          | 1.00×   |
| 2  | 148.2       | 233.8         | 26.0          | 1.86×   |
| 4  | 92.8        | 235.4         | 36.4          | 2.97×   |
| 8  | 53.5        | 235.7         | 56.0          | 5.15×   |
| 16 | 55.4        | 235.6         | 61.1          | 4.97×   |
| 32 | 61.2        | 235.9         | 75.6          | 4.50×   |

**Amdahl serial fraction:** ~0.22 → theoretical max ~4.5×  
**Bottleneck:** `t_compute` (MAP word-counting loop, 85%+ of worker time)

---

## Opt 1 — Combine phase + bytes regex + vectorised CRC32

**Branch:** `optimized-python-worker`  
**Commit:** `54be979`

### Changes
| Change | Description |
|--------|-------------|
| Bytes regex tokenizer | `re.compile(rb'[A-Za-z0-9]+')` replaces `line.split()` + `isalnum()`. Single pass over raw bytes — no UTF-8 decode, no per-line loop. |
| Read as raw bytes | `open(file, 'rb').read()` instead of `readlines()` as text. |
| Pre-aggregation (combine) | Count all occurrences into a single `dict` first; CRC32 called **once per unique word** instead of once per occurrence. |
| Vectorised CRC32 | `np.frompyfunc(zlib.crc32, 1, 1)` applied to numpy array of unique keys — reduced Python loop overhead for the dispatch step. |
| Batch writelines | `f.writelines(list)` instead of one `f.write()` per word. |

### Benchmark results (`amdhal_results_opt1.json`, 32 splits)

| N  | t_total (s) | t_compute (s) | t_shuffle (s) | vs baseline (total) | vs baseline (compute) |
|----|-------------|---------------|---------------|---------------------|-----------------------|
| 1  | 174.4       | 156.2         | 4.8           | **1.58×**           | **1.50×**             |
| 2  | 99.3        | 154.6         | 9.4           | **1.49×**           | 1.51×                 |
| 4  | 68.7        | 154.4         | 16.8          | **1.35×**           | 1.52×                 |
| 8  | 40.6        | 154.8         | 27.6          | **1.32×**           | 1.52×                 |
| 16 | 45.2        | 154.6         | 28.2          | **1.23×**           | 1.52×                 |
| 32 | 47.7        | 154.8         | 26.9          | **1.28×**           | 1.52×                 |

### Key observations
- **Compute**: consistent **1.52× faster** — combine phase removes redundant CRC32 calls for repeated words.
- **Shuffle**: **4–5× smaller** `t_shuffle` — partition files contain one line per unique word instead of one per occurrence, so `ssh cat` transfers far less data. This is a free bonus of the combine phase.
- **Scaling wall unchanged**: serial fraction still ~0.22; Amdahl limit still ~4.5×. The curve shape is the same, just lower in absolute time.

---

## Opt 2 — Parallel SSH shuffle

**Branch:** `optimized-python-worker`  
**Commit:** pending benchmark

### Changes
| Change | Description |
|--------|-------------|
| Parallel SSH | `ThreadPoolExecutor(max_workers=N)` launches all `ssh cat` fetches concurrently. Wall-clock `t_shuffle` = slowest single fetch instead of sum of all fetches. |
| Cleaner aggregation | Switched from `defaultdict(int)` to plain `dict.get()` (marginally faster). |
| Batch writelines | Output write uses generator with `writelines`. |

### Expected impact
- `t_shuffle` should drop from O(N × ssh_latency) to O(1 × ssh_latency).
- With N=8 and ~7s/fetch sequential → ~56s shuffle; parallel → ~7s shuffle.
- Reduces the REDUCE serial bottleneck significantly, should improve scaling at high N.

### Benchmark results
*Pending — run on lab machine and fill in.*

| N  | t_total (s) | t_compute (s) | t_shuffle (s) | vs opt1 (total) | vs baseline (total) |
|----|-------------|---------------|---------------|-----------------|---------------------|
| 1  |             |               |               |                 |                     |
| 2  |             |               |               |                 |                     |
| 4  |             |               |               |                 |                     |
| 8  |             |               |               |                 |                     |
| 16 |             |               |               |                 |                     |
| 32 |             |               |               |                 |                     |

---

## Potential future optimizations

| Idea | Expected gain | Complexity |
|------|---------------|------------|
| Gzip partition files before SSH transfer | 5–10× less network data | Low |
| Push partitions after MAP (avoid pull at REDUCE) | Eliminates SSH fan-in entirely | Medium |
| Replace SSH with direct TCP (netcat/socat) | ~50ms/conn SSH overhead gone | Medium |
| Multi-threaded MAP (one thread per reducer bucket) | Parallel I/O during write phase | Medium |
| Memory-mapped file read (`mmap`) | Faster large file I/O | Low |
| C++ worker (already done) | ~1.7× compute speedup | Done — see `cpp-worker` branch |
