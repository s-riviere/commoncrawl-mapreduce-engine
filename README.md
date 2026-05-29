# Distributed Computing Project — Challenge 1: Cluster Load Monitor

Deploys a lightweight TCP server on up to 50 live school machines, then
collects CPU load averages from every node and computes cluster-wide stats.

> **No jump host is used.** All SSH/SCP connections go **directly** from your
> machine to the lab machines over campus Wi-Fi (same network).  
> `ssh.enst.fr` is **not** involved and must **not** be used.

---

## Architecture

```
                  ┌─────────────────────────────────────┐
                  │  tp.telecom-paris.fr/ajax.php  (API) │
                  └──────────────┬──────────────────────┘
                                 │  get_machines.py
                                 ▼
                           machines.txt          ← 50 alive & free machines,
                                 │                 regenerated each deploy
                                 │
              ┌──────────────────┼──────────────────────┐
              │                  │                       │
         deploy.sh           client.py              kill.sh
              │                  │                       │
     1× SCP to any host    parallel TCP            parallel SSH
     (NFS: ~/server.py     to all hosts            pkill fired in bg,
      visible everywhere)  in machines.txt         SSH exits first
              │
     N× SSH → nohup python3 ~/server.py <port> &
```

**Why NFS for the upload?** Your home directory (`~/`) is mounted from a
Network File System shared across all lab machines. One `scp` to any
reachable host makes `server.py` instantly visible on all 50 machines —
no per-machine upload needed.

---

## Files

| File | Role |
|---|---|
| `get_machines.py` | Queries the school API, saves 50 alive & free hostnames to `machines.txt` |
| `machines.txt` | Live machine list — **auto-generated**, never edit by hand |
| `server.py` | Listens on the chosen port, replies with `load1 load5 load15` from `/proc/loadavg` |
| `client.py` | Connects in parallel to all hosts in `machines.txt`, prints per-node loads and cluster averages |
| `deploy.sh` | Phase 0: fetch machines. Phase 1: 1 SCP + parallel SSH bootstrap |
| `kill.sh` | Sends `pkill` to every node in parallel, removes `~/server.py` from NFS, verifies |

There is **no** `machines_alive.txt`. The single source of truth is `machines.txt`,
rebuilt from the API on every `./deploy.sh` run.

---

## Protocol

One TCP connection = one response, then server closes the connection.

```
client  ──── TCP connect ────────► server
client  ◄─── "l1 l5 l15\n" ─────  server  (floats, space-separated)
             (connection closed)
```

- `l1 / l5 / l15`: 1-min, 5-min, 15-min load averages (same as `uptime`)
- Source: `/proc/loadavg` on each lab machine
- Default port: **54321** — use a high, unique port to avoid conflicts with other students

---

## Prerequisites

1. **Campus Wi-Fi** (not eduroam — eduroam is on a different network from the lab machines).
2. **SSH key** set up so you can reach lab machines without a password:
   ```bash
   ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
   # Then copy your public key to any lab machine once:
   ssh-copy-id -i ~/.ssh/id_ed25519.pub <login>@tp-1a201-01.enst.fr
   # The key lands in ~/.ssh/authorized_keys on NFS → works on every machine
   ```
3. **Python 3** available locally (for `get_machines.py` and `client.py`).
4. No jump host config needed — direct SSH works from campus Wi-Fi.

---

## Usage

### Deploy

```bash
./deploy.sh 60012          # pick a port high enough to avoid collisions
```

Typical output:
```
Phase 0: queries API → saves 50 machines to machines.txt
Phase 1: 1 SCP upload, then parallel SSH nohup launch
  [STARTED] -> tp-1a201-08.enst.fr
  [STARTED] -> tp-1a201-09.enst.fr
  ...
  [FAILED]  -> tp-1a207-31.enst.fr   ← unreachable, ignore
```

### Check the cluster (anyone on campus Wi-Fi with machines.txt)

```bash
python3 client.py 60012
```

```
  tp-1a201-08.enst.fr     load: 0.32  0.27  0.13
  tp-1a201-09.enst.fr     load: 0.44  0.19  0.10
  ...
  tp-1a207-31.enst.fr     UNREACHABLE
  ═══════════════════════════════════════════════════
  Nodes responded: 48/50
  Avg cluster load  1 min : 0.2006
  Avg cluster load  5 min : 0.1644
  Avg cluster load 15 min : 0.1329
  ═══════════════════════════════════════════════════
```

Other group members only need `client.py` + a copy of `machines.txt`
to query the cluster — no deployment step required on their side.

### Kill and clean

```bash
./kill.sh 60012
```

- Sends `pkill -9` to every node **in parallel**, using a fire-and-forget
  pattern (pkill is backgrounded on the remote machine so SSH exits cleanly
  before the process is killed — this avoids false `[FAILED]` reports).
- Removes `~/server.py` from NFS.
- Waits 2 seconds then runs an automatic verification via `client.py`
  to confirm 0 nodes are still responding.

---

## Design notes

**Direct SSH, no jump host.**  
Campus Wi-Fi puts your machine in the same L3 network as `tp-*.enst.fr`.
Direct SSH works. Using `ssh.enst.fr` as a ProxyJump is explicitly
**forbidden** for this project.

**One SCP is enough.**  
NFS makes `~/server.py` visible on every lab machine the moment it lands
on any one of them.

**NFS vs local disk.**  
For this challenge the server writes nothing — it only reads `/proc/loadavg`.
Local disk will matter in the next challenge (MapReduce), where map output
must be written to `/tmp` (node-local) rather than `~/` (NFS), matching
Figure 1 of the Dean & Ghemawat paper.

**Port choice.**  
Pick a port above 10000 and coordinate with your group so you don't collide
with classmates. The port is passed as a CLI argument to all three scripts.

**`SO_REUSEADDR` on the server.**  
Allows the server to restart on the same port immediately after a kill,
without waiting for the OS `TIME_WAIT` timeout.

**Fire-and-forget kill pattern.**  
`pkill … & exit 0` on the remote side: pkill is forked in background,
SSH session exits with 0 before the kill happens. This gives reliable
`[CLEANED]` reports even on flaky campus Wi-Fi.

---

## Known limitations

- The API sometimes returns fewer than 100 machines (currently capped at 50
  alive & free). This is an API/infrastructure constraint, not a script bug.
- A few machines in room `1a207` and `1a222` are persistently unreachable
  at the network level — `[FAILED]` / `UNREACHABLE` on those is expected.
- Server is single-threaded (`accept()` loop). Sufficient for the protocol
  (one short response per connection), but not suitable for long-lived
  or concurrent requests.