---
marp: true
title: Distributed MapReduce on Common Crawl
paginate: true
---

# Distributed MapReduce on Common Crawl
### SLR207 — Télécom Paris

A from-scratch MapReduce engine (Python) on the lab cluster
+ fault tolerance, performance, 4 analyses, Kafka Streams comparison

*(10 min talk + 10 min questions)*

---

## Agenda

1. Architecture & storage model
2. Protocol (V1 KISS → V2 fault-tolerant)
3. Performance metrics & Amdahl's law
4. Pain points — what broke & why
5. Three use cases beyond word count
6. Batch vs stream: us vs Hadoop vs Kafka Streams
7. Live demo

---

## 1. Architecture

- **Main / Workers** over SSH (no HDFS, no YARN, no root).
- **Main** = task queues + `MAP→REDUCE` barrier + failure detector. Routes *metadata only*.
- **Workers** = MAP to **local `/tmp`**, REDUCE pulls partitions worker↔worker via `ssh cat` (the "remote read").
- Storage rule: **intermediates on local disk**, final output on NFS, **never** read hundreds of files from NFS.

---

## Common Crawl ingestion

- WET (plain-text) files from `data.commoncrawl.org`; file list = `wet.paths.gz`.
- Two paths:
  - pre-download to NFS (`download_commoncrawl.py --missing-only`),
  - **direct from Amazon** (`master --crawl <ID>` → workers stream+gunzip to `/tmp`).
- Direct read = **NFS untouched** → solves the "poor little NFS server" problem.
- Why care? Common Crawl is a primary data source for training LLMs.

---

## 2. Protocol — V1 (KISS)

- TCP, newline-delimited **JSON**, one connection per worker.
- `READY → {MAP|REDUCE|WAIT} → TASK_FINISHED → ACK`.
- Reducer for key `k`: `crc32(k) % n_reducers`.
- Barrier: all MAP done → REDUCE.
- Diagram: `doc/map_reduce/MapReduce.svg` (sequencediagram.org).

---

## 2. Protocol — V2 (fault tolerance)

- **Heartbeat (2 s) + lease (10 s)** failure detector.
- Re-execution (paper §3.1):
  - dead in-progress task → re-queue,
  - dead **completed MAP** → **re-run** (its `/tmp` is gone),
  - completed REDUCE → **kept** (atomic on NFS).
- **Atomic output**: `.tmp` + `os.replace()` → exactly-once.
- **Straggler backup tasks** (§3.6): duplicate the slowest in-flight map, first wins.

---

## 3. Performance — what we time

- Master: `t_total, t_map, t_reduce`.
- Worker: `t_download, t_clean, t_io_read, t_compute, t_shuffle, t_io_write`.
- **Measured bottleneck:** MAP `t_compute` > 85 % of worker time.
- Optimisations: bytes-regex + combine + vectorised CRC32 (**compute 1.52×**, shuffle 4–5× smaller) + **parallel SSH shuffle** (`ControlMaster`).

---

## 4. Amdahl's law (same dataset at every point)

Reproducible single-machine sweep (`tests/run_all.py`, 8 splits):

| N | t_total | Speedup |
|---|---------|---------|
| 1 | 1.65 s | 1.00× |
| 2 | 1.02 s | 1.61× |
| 4 | 0.84 s | **1.96×** |

- Serial fraction **f ≈ 0.34** on one box → ceiling **≈ 3×** (shared mem bandwidth).
- Cluster sweep (`amdahl_bench.py`, N→32 independent nodes): ceiling rises **~4–5×**.
- ≥2 workers ⇒ **measure**, never extrapolate. Figure: `runtime/amdahl_speedup.png`.

---

## 5. Pain points

- NFS overload → local `/tmp` + direct Amazon read.
- SSH fan-in / `fail2ban` → `ControlMaster` multiplexing + `max_ssh≈8`.
- **One dead worker froze the whole job (V1)** → V2 heartbeats + re-exec.
- Partial reduce file on crash → atomic commit.
- "Why only a few ×?" → Amdahl, the hard way.

---

## 6. Three use cases (same engine, `-j`)

- **`lang`** — language popularity/ranking (stop-word hits). English dominates.
- **`wordlen`** — word-length distribution (size). Unimodal, peak ~2–4 chars.
- **`bigram`** — phrase popularity. Function-word pairs top the list.
- All validated against a **single-machine reference** (`validate.py`).

---

## 7. Batch vs Stream

| | Us | Hadoop | Kafka Streams |
|---|---|---|---|
| Model | Batch | Batch | **Stream** |
| Storage | NFS+/tmp | **HDFS** | Kafka log |
| Output | final once | final once | **changelog (live)** |
| Setup | easy | hard | medium (no root/Docker) |

- We lack: HDFS, YARN, speculative exec, secure RPC.
- Kafka WordCount = same final counts, but **never "done"**.
- **Common Crawl → Kafka source connector** (`commoncrawl_source.sh`): streams S3/HTTPS straight into the topic — no NFS, no file.

---

## Live demo

1. `deploy_commoncrawl.sh -w 8 -r 8 -s 20` → watch MAP→REDUCE.
2. In a 2nd terminal: `fault_tolerance_demo.sh -n 1` → master recovers.
3. `validate.py` → output still correct ✅.
4. `kafka/deploy_kafka.sh` + `run_wordcount.sh --crawl CC-MAIN-2024-10` (direct S3 source).

**No cluster? Run it solo:** `python3 tests/run_all.py` reproduces the whole
pipeline (4 analyses + fault tolerance + Amdahl figure) on one laptop, 9/9 checks.
Or drive it by hand: `scripts/local_cluster.sh -i <splits> -n 4 --validate`.

---

## Thank you — questions?

Repo: `README.md` (run from scratch) · `report/` · `self-assessment.md`
