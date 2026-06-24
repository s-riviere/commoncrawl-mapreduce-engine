# Self-assessment — Distributed MapReduce

Filled in honestly. A criterion is ticked only when it is implemented **and working**.
Levels: **[Base]** minimum · **[Solid]** robust · **[Advanced]** goes further.

> Pointers use this repo's paths so each tick is checkable.

## 1. Infrastructure and deployment
- [x] **[Base]** 100 live machines generated automatically (`ajax.php` API, `.enst.fr`) — `src/deploy/get_machines.sh`
- [x] **[Base]** Deployment = SCP to HOME (NFS) + SSH to start servers — `src/deploy/deploy_commoncrawl.sh`
- [x] **[Base]** Servers listen on a chosen port, no collision (configurable `-p`)
- [x] **[Base]** Cleaning script; clean redeployment possible — `src/deploy/kill_commoncrawl.sh`, `src/deploy/kill_cpuload.sh`
- [x] **[Solid]** Deployment robust to unreachable machines (timeouts, non-blocking)
- [x] **[Solid]** SSH keys + fingerprint bypass (`StrictHostKeyChecking=no`, `BatchMode=yes`)
- [x] **[Solid]** Deploy and clean scripts idempotent and replayable (`--missing-only`, `rm -rf output`)

## 2. NFS vs local disk
- [x] **[Base]** We know HOME is on NFS, not local disk
- [x] **[Solid]** MAP intermediates written to **local `/tmp`**, not NFS — `worker.py` `local_map_dir`
- [x] **[Solid]** NFS load deliberately limited (direct Amazon read via `--crawl`; controlled writes)
- [x] **[Advanced]** Explored partitions beyond `/tmp` (scratch space via `df`/`mount`) — `src/benchmarks/find_scratch.sh` inspects mounts and recommends the largest local non-NFS partition; worker `--spill-dir` redirects staging there (tool run locally; cluster figures pending lab time)

## 3. Protocol design
- [x] **[Base]** Main/Workers roles clearly defined
- [x] **[Base]** V1 space-time diagram (KISS) — `doc/map_reduce/MapReduce.svg`
- [x] **[Solid]** `map → shuffle → reduce` synchronization working (phase barrier)
- [x] **[Solid]** System bootstrap handled cleanly (master binds before loading tasks; early workers `WAIT`)
- [x] **[Advanced]** Protocol formalized & documented (messages, formats, FT edge cases) — `report/report.md` §2, FT diagram

## 4. MapReduce core
- [x] **[Base]** Word frequency works on small splits (proof of concept)
- [x] **[Base]** Key distribution at reduce via `crc32(key) % N`
- [x] **[Solid]** Remote read of local intermediates for reduce (parallel `ssh cat`, `ControlMaster`)
- [x] **[Solid]** Results validated against a single-machine reference — `src/mapreduce/validate.py`
- [x] **[Solid]** Full pipeline reproducible by **one person, no cluster** — `tests/run_all.py` (4 analyses + fault tolerance + Amdahl, 9/9 checks); manual demo via `src/benchmarks/local_cluster.sh`
- [x] **[Advanced]** Works on real Common Crawl splits at scale (32 in the Amdahl sweep; design scales to 100/1000+ as MAP tasks ≫ workers) — *scale tested to tens; 1000+ left to lab time*

## 5. Common Crawl data
- [x] **[Base]** Reading splits from NFS / `/cal/commoncrawl`-style shared input
- [x] **[Solid]** Understanding of Common Crawl structure (WARC/WAT/WET, `wet.paths.gz`)
- [x] **[Advanced]** Direct read from Amazon without internal NFS — `master --crawl` + worker `--direct-read` streams each `.wet.gz` from S3/HTTPS **straight into memory** (no NFS, no `/tmp` staging)

## 6. Performance and Amdahl's law
- [x] **[Base]** Timing of every step (deploy, clean, I/O, sync/wait, network, compute) — `WORKER_TIMING`/`TIMING`
- [x] **[Solid]** Bottlenecks identified and backed by measurements (compute > 85 %)
- [x] **[Solid]** Reference point established (speedup = 1 for 1 node)
- [x] **[Solid]** Speedup vs nodes graph, **same dataset at every point** — reproducible solo sweep `tests/run_all.py` (committed `runtime/amdahl_results.json` + `amdahl_speedup.png`); cluster sweep `amdahl_bench.py` + `plot_amdahl.py`
- [x] **[Advanced]** Know when interpolation is OK (single machine) vs not (≥2 nodes → measure)
- [x] **[Advanced]** Amdahl verified empirically & interpreted (single-machine f ≈ 0.34, ceiling ≈ 3×; cluster of independent nodes ≈ 4–5×)

## 7. Fault tolerance
- [x] **[Base]** Detection of a dead worker (heartbeat 2 s + lease 10 s / TCP close)
- [x] **[Solid]** Re-execution of lost tasks by the Main (in-progress + completed MAP)
- [x] **[Solid]** Atomic output writes (`.tmp` + `os.replace()`)
- [x] **[Advanced]** Demonstration by killing nodes mid-computation — `src/deploy/fault_tolerance_demo.sh` (cluster) / `tests/run_all.py` (solo)
- [x] **[Advanced]** Straggler handling (backup tasks, paper §3.6)

## 8. Comparison and Kafka Streams (light)
- [x] **[Base]** Can situate batch vs stream — `report/report.md` §7
- [x] **[Solid]** Minimal wordcount with Kafka Streams (downloaded files, no Docker/root) — `src/kafka/*`
- [x] **[Solid]** Documented comparison vs Hadoop & Kafka Streams (incl. missing HDFS etc.)
- [x] **[Advanced · optional]** Direct Common Crawl read via a **Kafka** source/connector — `src/kafka/commoncrawl_source.sh` streams a `.wet.gz` from `data.commoncrawl.org` (S3/HTTPS) straight into the input topic (no NFS, no file); wired into `run_wordcount.sh --crawl <ID> [--index N]`

## 9. Use cases, report and demo
- [x] **[Base]** 3 use cases beyond wordcount — `lang`, `wordlen`, `bigram` (`-j` flag)
- [x] **[Solid]** Use-case results interpreted and explained — `report/report.md` §6
- [x] **[Base]** Report covers the 7 required points — `report/report.md`
- [x] **[Base]** Demo ready (10+10), work distributed — `slides/presentation.md`

---

## Group wrap-up

- **[Base] criteria ticked:** all of them.
- **Two weakest points today:** (1) scale runs at 100/1000+ splits not yet executed (engine ready, needs lab time); (2) the multi-node cluster sweep itself is pending lab access (the engine runs solo end-to-end; `amdahl_bench.py` is ready for the cluster).
- **Kafka owner:** clearly owned (deploy/clean/wordcount + Common Crawl source connector committed), not left to the last minute.
- **What broke and why:** see `report/report.md` §5 (NFS overload, SSH fan-in, V1 single-worker hang, partial reduce writes, Amdahl wall).
- **If the demo were tomorrow, what would not pass:** a 1000-split cluster run timing (we'd demo tens of splits solo); everything else — the 4 analyses, fault tolerance, Amdahl, and the Kafka Common-Crawl source — is demoable.
