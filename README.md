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
                           machines.txt          ← regenerated fresh each deploy
                                 │
              ┌──────────────────┼───────────────────────────┐
              │                  │                           │
         deploy.sh           client.py                  kill.sh
              │                  │                           │
     1× SCP to any host    parallel TCP               parallel SSH
     uploads server.py     to all hosts               pkill fired in bg,
     AND machines.txt      in machines.txt            SSH exits first
     to NFS (~/),               │
     so team can sync      optional --sync flag:
                           scp ~/machines.txt from
                           NFS before querying
```

**Why NFS for the upload?** Your home directory (`~/`) is mounted from a
Network File System shared across all lab machines. One `scp` to any
reachable host makes both `server.py` and `machines.txt` instantly visible
on all 50 machines — no per-machine upload needed.

---

## The Team Sync Problem (and the fix)

`deploy.sh` regenerates `machines.txt` fresh from the API on every run.
The set of live machines changes each session. If team members use a stale
`machines.txt` from git, their `client.py` connects to the wrong set of
machines.

**Fix:** `deploy.sh` uploads `machines.txt` to NFS alongside `server.py`.
Team members run `--sync` once after each deployment:

```bash
# Person who deployed:
./deploy.sh 60012
# → uploads server.py AND machines.txt to ~/  on NFS
# → at the end, prints the exact --sync command to share with the team

# Every other team member (once per deployment):
python3 client.py 60012 --sync tp-1a201-08.enst.fr
# → fetches ~/machines.txt from NFS, saves it locally, then queries
```

The deploy person shares the `--sync <host>` command (it's printed at the
end of `./deploy.sh` output). After that first sync, team members can run
`python3 client.py 60012` normally without `--sync`.

---

## Files

| File | Role |
|---|---|
| `get_machines.py` | Queries the school API, saves alive & free hostnames to `machines.txt` |
| `machines.txt` | Live machine list — **auto-generated**, never edit by hand, do not rely on git version |
| `server.py` | Listens on the chosen port, replies with `load1 load5 load15` |
| `client.py` | Queries all hosts in parallel; `--sync <host>` pulls fresh `machines.txt` from NFS |
| `deploy.sh` | Phase 0: fetch machines via API. Phase 1: 1 SCP (server.py + machines.txt) + parallel SSH |
| `kill.sh` | Sends `pkill` to every node in parallel, removes NFS files, verifies |

There is **no** `machines_alive.txt`. The single source of truth is `machines.txt`,
rebuilt from the API on every `./deploy.sh` run and synced to NFS.

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
- Default port: **54321** — use a high unique port to avoid conflicts with other students

---

## Prerequisites

1. **Campus Wi-Fi** (not eduroam — eduroam is on a different network from the lab machines).
2. **SSH key** set up so you can reach lab machines without a password:
   ```bash
   ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
   # Copy your public key to any lab machine once —
   # NFS propagates it to all machines automatically:
   ssh-copy-id -i ~/.ssh/id_ed25519.pub <login>@tp-1a201-01.enst.fr
   ```
3. **Python 3** available locally.
4. No jump host config needed — direct SSH works from campus Wi-Fi.

---

## Usage

### Person deploying

```bash
./deploy.sh 60012
```

At the end of the output:
```
=================================================
 Deployment complete.

 Verify:      python3 client.py 60012
 Team sync:   python3 client.py 60012 --sync tp-1a201-08.enst.fr
=================================================
```

Share the `Team sync:` line with your group.

### Verify (deploy person)

```bash
python3 client.py 60012
```

### Other team members — first time after each deployment

```bash
python3 client.py 60012 --sync tp-1a201-08.enst.fr
# Syncing machines.txt from NFS via tp-1a201-08.enst.fr ...
#   [OK] machines.txt updated from NFS.
#
# Connecting to 50 machines on port 60012...
# ...
```

After the sync, the local `machines.txt` is up to date and subsequent runs
work without `--sync`.

### Kill and clean

```bash
./kill.sh 60012
```

- Sends `pkill -9` to every node in parallel (fire-and-forget — see below).
- Removes `~/server.py` and `~/machines.txt` from NFS.
- Auto-verifies: re-runs `client.py` after 2 seconds to confirm 0 nodes respond.

---

## Design notes

**Direct SSH, no jump host.**  
Campus Wi-Fi puts your machine in the same L3 network as `tp-*.enst.fr`.
Using `ssh.enst.fr` as a ProxyJump is explicitly **forbidden**.

**One SCP uploads both files.**  
`scp server.py machines.txt <host>:~/` — both land on NFS, both visible
everywhere. The `--sync` flag in `client.py` does the reverse: pulls
`~/machines.txt` from NFS to local disk.

**NFS vs local disk.**  
For this challenge the server writes nothing (only reads `/proc/loadavg`).
Local disk matters in the next challenge (MapReduce), where map output must
go to `/tmp` (node-local) rather than `~/` (NFS), matching Figure 1 of
the Dean & Ghemawat paper.

**Fire-and-forget kill pattern.**  
`pkill … & exit 0` on the remote side: pkill is forked in background, SSH
exits with 0 before the process is killed. This gives reliable `[CLEANED]`
reports even when killing a process disrupts its own SSH tunnel.

**`SO_REUSEADDR` on the server.**  
Allows restarting on the same port immediately after a kill without waiting
for the OS `TIME_WAIT` timeout.

---

## Known limitations

- The API currently returns ~50 alive & free machines. This is an
  infrastructure constraint, not a script bug.
- A few machines in rooms `1a207` and `1a222` are persistently unreachable —
  `[FAILED]` / `UNREACHABLE` on those is expected.
- Server is single-threaded (`accept()` loop). Fine for this protocol
  (one short response per connection), not suitable for concurrent requests.