# Distributed Computing Project (SLR)

Ce depot contient deux workflows principaux sur les machines TP Telecom Paris:

- Challenge CPU load: deploiement d'un serveur TCP minimal sur plusieurs noeuds, puis collecte parallele des load averages.
- Mini framework MapReduce: deploiement de workers generiques, execution d'un job dynamique (ex: wordcount).

Le README precedent etait centre sur un ancien workflow; cette version decrit le comportement actuel du code present dans le depot.

## Vue d'ensemble

1. scripts/deploy.sh
   - Selectionne des machines via src/common/get_machines.py.
   - Uploade les fichiers necessaires (NFS + /tmp) sur un hote reachable.
   - Lance un process distant sur chaque machine de runtime/machines.txt.
   - Supporte deux types de deploiement: cpu_load et wordcount.
2. scripts/kill.sh
   - Tue les process qui ecoutent sur le port cible.
   - Supprime les repertoires distants /tmp/slr207-group1-bis et ~/slr207-group1-bis.
3. Clients
   - src/cpu_load/client.py pour requeter la charge CPU.
   - src/map_reduce/master.py pour orchestrer MAP -> SHUFFLE -> REDUCE.

## Prerequis

1. Compte Telecom Paris actif et acces SSH sur les machines TP.
2. Connexion au reseau campus (API + SSH).
3. Cle SSH configuree sur les machines TP.
4. Python 3.10+ recommande.

Installation locale:

```bash
pip install -e .
```

Exemple de config SSH:

```sshconfig
Host tp-*
  User <votre_login>
  PreferredAuthentications publickey
  IdentityFile ~/.ssh/<votre_cle>
```

## Workflow A - Charge CPU distribuee

### 1) Deployer les serveurs

```bash
./scripts/deploy.sh cpu_load
./scripts/deploy.sh cpu_load 54555
```

Le script:

- Genere/rafraichit la liste des machines.
- Uploade src/cpu_load/server.py et runtime/machines.txt.
- Lance worker serveur sur toutes les machines en parallele.

### 2) Interroger le cluster

Depuis la machine qui a deploye:

```bash
python3 src/cpu_load/client.py
python3 src/cpu_load/client.py 54555
```

Depuis un autre poste (avec sync du machines.txt):

```bash
python3 src/cpu_load/client.py 54321 --sync <host_reference>
```

Le client effectue les requetes en parallele (ThreadPoolExecutor), affiche les noeuds joignables/non joignables, puis les moyennes cluster 1/5/15 min.

### 3) Nettoyer

```bash
./scripts/kill.sh
./scripts/kill.sh 54555
```

## Workflow B - MapReduce (job dynamique)

### 1) Deployer les workers

```bash
./scripts/deploy.sh wordcount
./scripts/deploy.sh wordcount 55000
```

Le deploiement wordcount uploade:

- src/map_reduce/worker.py
- src/map_reduce/wordcount.py

Puis lance worker.py sur chaque machine cible.

### 2) Lancer un job depuis le master

```bash
python3 src/map_reduce/master.py wordcount
python3 src/map_reduce/master.py wordcount 55000
python3 src/map_reduce/master.py wordcount 55000 --sync <host_reference>
```

Le master:

- Distribue des taches MAP (texte -> paires cle/valeur).
- Realise le SHUFFLE localement.
- Distribue des taches REDUCE.
- Affiche le resultat final trie.

### 3) Nettoyer les workers

```bash
./scripts/kill.sh 55000
```

## Protocoles reseau

CPU load (src/cpu_load/server.py):

- 1 connexion TCP -> 1 ligne "load1 load5 load15\n" puis fermeture.
- Socket IPv6 dual-stack, SO_REUSEADDR active.

MapReduce (src/map_reduce/worker.py):

- 1 connexion TCP -> 1 payload JSON termine par \n.
- Format: {"task": "MAP|REDUCE", "job_name": "module", "data": ...}
- Reponse JSON: {"status": "OK|ERROR", "result"|"message": ...}

## Fichiers importants

- scripts/deploy.sh: pipeline de deploiement multi-type (cpu_load/wordcount).
- scripts/kill.sh: cleanup distant par port.
- src/common/get_machines.py: collecte API Telecom et generation de machines.txt.
- src/cpu_load/server.py: serveur de loadavg.
- src/cpu_load/client.py: client parallele + option --sync.
- src/map_reduce/worker.py: worker generique chargeant dynamiquement un module job.
- src/map_reduce/master.py: orchestration MAP/SHUFFLE/REDUCE.
- src/map_reduce/wordcount.py: exemple de job.
- src/common/bench.py et scripts/bench.sh: instrumentation timing.
