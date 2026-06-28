# Distributed Computing Project (SLR)

Projet de calcul distribué sur les machines de TP Telecom Paris avec deux workflows principaux:

1. CPU Load: déploiement d'un serveur TCP minimal sur plusieurs noeuds, puis interrogation parallèle des load averages.
2. CommonCrawl + MapReduce: pipeline MAP/SHUFFLE/REDUCE distribué pour compter les mots sur des splits CommonCrawl.

Ce README décrit l'état actuel du dépôt et les scripts utilisés en pratique.

## 0) Démarrage rapide — reproduire sur le cluster Telecom

> **Le cœur du projet** est le pipeline MapReduce **distribué en SSH** sur les machines
> de TP. Le déploiement est entièrement automatisé : un seul script choisit les machines,
> télécharge les splits CommonCrawl, démarre le master et les N workers, puis stream les
> logs jusqu'à la fin du job.

```bash
# 1. Récupérer le code (sur une machine ayant accès SSH au cluster Telecom)
git clone https://github.com/kabilaymen/Distributed-Computing-Project.git
cd Distributed-Computing-Project
pip install -e .

# 2. Déployer un wordcount distribué : 8 workers, 8 reducers, 8 splits CommonCrawl.
#    Le script affiche le master choisi (tp-XXXX) en Phase 1 et, en fin de job, une
#    ligne « TIMING: {"t_total": ..., "t_map": ..., "t_reduce": ...} » (cf. §5.2).
bash src/deploy/deploy_commoncrawl.sh -w 8 -r 8 -s 8 -j wordcount

# 3. Valider : recalcul mono-machine de référence vs sortie distribuée (cf. §12).
#    Remplacer tp-XXXX par le master affiché à l'étape 2 ; -s = même nb de splits.
ssh tp-XXXX "python3 ~/slr207-group1-commoncrawl-$USER/validate.py \
    -i ~/slr207-group1-commoncrawl-$USER/input \
    -o ~/slr207-group1-commoncrawl-$USER/output -j wordcount -s 8"
# -> se termine par « [VALIDATE] ✅ PASS »

# 4. Nettoyer le cluster
bash src/deploy/kill_commoncrawl.sh
```

Le reste du README détaille chaque étape : déploiement (§5), sweep d'Amdahl multi-nœuds
(§9.1), tolérance aux pannes (§10), validation (§12), nettoyage (§13), Kafka (§14).

> **Pas d'accès au cluster ?** Un harnais mono-machine rejoue tout le pipeline en local
> en une commande (`python3 tests/run_all.py`) — voir §2bis.

## Table des matières

- [Distributed Computing Project (SLR)](#distributed-computing-project-slr)
  - [Table des matières](#table-des-matières)
  - [0) Démarrage rapide — cluster Telecom](#0-démarrage-rapide--reproduire-sur-le-cluster-telecom)
  - [1) Vue d'ensemble](#1-vue-densemble)
  - [2) Prérequis](#2-prérequis)
  - [2bis) Test solo sans cluster (optionnel)](#2bis-test-solo-sans-cluster-optionnel)
  - [3) Structure du dépôt](#3-structure-du-dépôt)
  - [4) Workflow A - CPU Load](#4-workflow-a---cpu-load)
    - [4.1 Objectif](#41-objectif)
    - [4.2 Déploiement](#42-déploiement)
    - [4.3 Interrogation du cluster](#43-interrogation-du-cluster)
    - [4.4 Arrêt et nettoyage](#44-arrêt-et-nettoyage)
    - [4.5 Protocole réseau CPU Load](#45-protocole-réseau-cpu-load)
  - [5) Workflow B - CommonCrawl + MapReduce](#5-workflow-b---commoncrawl--mapreduce)
    - [5.1 Objectif](#51-objectif)
    - [5.2 Déploiement complet](#52-déploiement-complet)
    - [5.3 Ce que fait le script de déploiement](#53-ce-que-fait-le-script-de-déploiement)
    - [5.4 Téléchargement CommonCrawl](#54-téléchargement-commoncrawl)
    - [5.5 Cycle d'exécution MapReduce](#55-cycle-dexécution-mapreduce)
    - [5.6 Détails de traitement](#56-détails-de-traitement)
  - [6) Chemins de données et stockage](#6-chemins-de-données-et-stockage)
  - [7) Protocole de messages MapReduce](#7-protocole-de-messages-mapreduce)
  - [8) Observabilité et logs](#8-observabilité-et-logs)
  - [9) Performance et bench](#9-performance-et-bench)
  - [10) Tolérance aux pannes](#10-tolérance-aux-pannes)
  - [11) Use cases (jobs) au-delà du wordcount](#11-use-cases-jobs-au-delà-du-wordcount)
  - [12) Validation des résultats](#12-validation-des-résultats)
  - [13) Nettoyage](#13-nettoyage)
  - [14) Comparaison Kafka Streams](#14-comparaison-kafka-streams)
  - [15) Livrables](#15-livrables)
  - [16) Limites et choix techniques](#16-limites-et-choix-techniques)

## 1) Vue d'ensemble

Le projet est organisé autour de scripts shell qui automatisent le déploiement sur des machines distantes accessibles en SSH.

- Sélection des machines: [src/deploy/get_machines.sh](src/deploy/get_machines.sh)
- Déploiement CPU Load: [src/deploy/deploy_cpuload.sh](src/deploy/deploy_cpuload.sh)
- Arrêt/cleanup CPU Load: [src/deploy/kill_cpuload.sh](src/deploy/kill_cpuload.sh)
- Déploiement CommonCrawl + MapReduce: [src/deploy/deploy_commoncrawl.sh](src/deploy/deploy_commoncrawl.sh)
- Outils de bench shell: [src/benchmarks/bench.sh](src/benchmarks/bench.sh)

Le pipeline MapReduce actif est dans [src/mapreduce](src/mapreduce): master/worker et utilitaires CommonCrawl.

## 2) Prérequis

1. Compte Telecom Paris actif avec accès SSH aux machines de TP.
2. Connexion réseau campus (API et SSH).
3. Clé SSH configurée pour accès non interactif.
4. Python 3.10+ recommandé.
5. Outils shell usuels: ssh, scp, timeout, curl, jq.

Installation locale:

```bash
pip install -e .
```

Configuration SSH:

```sshconfig
Host tp-*
  User <votre_login>
  PreferredAuthentications publickey
  IdentityFile ~/.ssh/<votre_cle>
```

## 2bis) Test solo sans cluster (optionnel)

Pour valider le code **sans accès au cluster Telecom**, le harnais
[tests/run_all.py](tests/run_all.py) rejoue tout le pipeline sur une seule machine : il
lance le master + N workers comme processus séparés sur `localhost`, avec shuffle en
lecture disque locale (**aucun `sshd` requis**). Le moteur (`master.py` / `worker.py`)
est strictement le même qu'en multi-nœuds ; seul le transport du shuffle change.

```bash
python3 tests/run_all.py          # suite complète (~1 min)
python3 tests/run_all.py --quick  # version rapide (datasets réduits)
```

Couvre, sans intervention : correction des 4 analyses (`wordcount`, `lang`, `wordlen`,
`bigram`) vs référence mono-machine, tolérance aux pannes (worker tué en plein job), et
un mini sweep d'Amdahl (N = 1, 2, 4) qui régénère `runtime/amdahl_speedup.png`. Code de
sortie `0` = tout PASS. `matplotlib`/`numpy` sont optionnels (repli Python pur, figure
non rendue) ; installez-les dans un venv pour produire la figure.

## 3) Structure du dépôt

```text
src/
  deploy/
    get_machines.sh         # sélection des meilleures machines via API ajax.php
    get_machines.py         # variante Python de la sélection
    deploy_cpuload.sh       # déploiement parallèle CPU Load
    kill_cpuload.sh         # arrêt + nettoyage distant CPU Load
    deploy_commoncrawl.sh   # déploiement complet CommonCrawl + MapReduce
    kill_commoncrawl.sh     # arrêt + nettoyage distant MapReduce
    fault_tolerance_demo.sh # injection de panne (kill worker) pour démo FT
  server/
    server.py               # serveur TCP renvoyant load averages (CPU Load)
  client/
    client.py               # client parallèle + rapport agrégat (CPU Load)
  mapreduce/
    master.py               # master (tolérant aux pannes)
    worker.py               # worker (heartbeat, jobs, écriture atomique)
    validate.py             # vérif. résultat distribué vs calcul mono-machine
    download_commoncrawl.py # téléchargement WET en splits texte
  kafka/
    deploy_kafka.sh         # broker Kafka single-node (KRaft, sans Docker/root)
    run_wordcount.sh        # démo Kafka Streams WordCount sur un split (ou --crawl)
    commoncrawl_source.sh   # connecteur source : Common Crawl S3/HTTPS -> topic Kafka
    clean_kafka.sh          # teardown du broker Kafka
  benchmarks/
    amdahl_bench.py         # sweep Amdahl multi-nœuds (SSH)
    amdahl_cluster_sweep.sh # sweep Amdahl sur le cluster (N=1,2,4,8,16)
    plot_amdahl.py          # rendu du graphe Amdahl (speedup vs nœuds)
    ssh_parallelism_sweep.py# balayage du parallélisme SSH
    demo_verified_run.sh    # déploiement démo (liste de machines vérifiées)
    local_cluster.sh        # cluster MapReduce manuel sur une seule machine
    find_scratch.sh         # exploration df/mount : recommande une partition scratch
    bench.sh                # instrumentation timing shell
    bench.py                # utilitaire Python d'instrumentation

tests/
  run_all.py                # harnais de test solo (correction + FT + Amdahl)

runtime/
  machines.txt              # liste des machines cibles (générée, gitignored)

report/
  final_report.pdf          # rapport (7 points demandés)
  final_report.tex          # source LaTeX du rapport
  amdahl_results.json       # données du graphe Amdahl solo (livré)
  amdahl_speedup.png        # graphe Amdahl solo (livré)
  amdahl_cluster.json       # données du sweep Amdahl cluster (livré)
  amdahl_cluster.png        # graphe Amdahl cluster (livré)
slides/
  presentation.md           # support de démo (10 min)
self-assessment.md          # questionnaire d'auto-évaluation rempli

doc/
  optimizations.md
  fault_tolerance/
  map_reduce/
  guidelines/
  evaluation/
```

## 4) Workflow A - CPU Load

### 4.1 Objectif

Démarrer un petit serveur TCP sur plusieurs machines et collecter la charge CPU moyenne (1/5/15 min) en parallèle.

### 4.2 Déploiement

```bash
bash src/deploy/deploy_cpuload.sh
bash src/deploy/deploy_cpuload.sh 54555
```

Le script:

1. Génère [runtime/machines.txt](runtime/machines.txt) via [src/deploy/get_machines.sh](src/deploy/get_machines.sh).
2. Uploade [src/server/server.py](src/server/server.py) sur NFS distant.
3. Démarre un serveur par machine en parallèle.

### 4.3 Interrogation du cluster

```bash
python3 src/client/client.py
python3 src/client/client.py 54555
python3 src/client/client.py 54555 --sync <host_reference>
```

Le client:

- Interroge les noeuds en parallèle (ThreadPoolExecutor).
- Affiche noeuds joignables/non joignables.
- Calcule les moyennes cluster sur 1/5/15 minutes.

### 4.4 Arrêt et nettoyage

```bash
bash src/deploy/kill_cpuload.sh
bash src/deploy/kill_cpuload.sh 54555
```

Le cleanup:

- Tue le process écoutant sur le port cible.
- Supprime les répertoires distants utilisés par le workflow CPU Load.

### 4.5 Protocole réseau CPU Load

- Connexion TCP vers un worker.
- Réponse texte unique: "load1 load5 load15" puis fermeture.

## 5) Workflow B - CommonCrawl + MapReduce

### 5.1 Objectif

Exécuter un wordcount distribué sur des fichiers CommonCrawl split en mode MAP/SHUFFLE/REDUCE.

### 5.2 Déploiement complet

```bash
bash src/deploy/deploy_commoncrawl.sh
bash src/deploy/deploy_commoncrawl.sh -p 60000 -w 8 -r 8 -s 20
bash src/deploy/deploy_commoncrawl.sh -w 8 -r 8 -s 20 -j lang
```

Paramètres principaux:

- -p: port master (défaut 54321)
- -w: nombre de workers (défaut 10)
- -r: nombre de reducers (défaut 10)
- -s: nombre de splits CommonCrawl cibles (défaut 10)
- -j: analyse à exécuter: `wordcount` (défaut) | `lang` | `wordlen` | `bigram`

**Exemple complet** (8 workers, 8 reducers, 8 splits, wordcount) :

```bash
bash src/deploy/deploy_commoncrawl.sh -p 54321 -w 8 -r 8 -s 8 -j wordcount
```

**Exemple de sortie** (abrégé — noté le nom du master `tp-XXXX` affiché en Phase 1) :

```text
 Phase 0 : Fetching Alive Machines from API
 Phase 1 : NFS Upload & Master Bootstrap
Master infrastructure successfully established on tp-1a201-25:54321.
 Phase 2 : Workers Bootstrap
 Phase 3 : Monitoring MapReduce Execution
 ... (logs master en direct) ...
TIMING: {"t_total": 84.60, "t_map": 61.2, "t_reduce": 23.4}
```

La dernière ligne `TIMING: {...}` (JSON) est exploitable pour la loi d'Amdahl (§9).
Press `Ctrl+C` pendant la Phase 3 détache seulement l'affichage ; le job continue.

### 5.3 Ce que fait le script de déploiement

1. Récupère N+1 machines (1 master + N workers).
2. Uploade [src/mapreduce/master.py](src/mapreduce/master.py), [src/mapreduce/worker.py](src/mapreduce/worker.py), [src/mapreduce/download_commoncrawl.py](src/mapreduce/download_commoncrawl.py) vers NFS distant.
3. Vérifie le stock de splits et télécharge seulement les manquants si nécessaire.
4. Démarre le master (unbuffered, logs redirigés).
5. Démarre les workers en parallèle.
6. Stream les logs master en direct jusqu'à la fin du job.
7. Quand le master se termine (fin normale) ou est tué, les workers détectent la fermeture de la socket et s'arrêtent automatiquement.

### 5.4 Téléchargement CommonCrawl

Le téléchargement transforme les WET gzip en fichiers texte locaux nommés:

- commoncrawl-0000.txt
- commoncrawl-0001.txt
- ...

Le mode --missing-only évite de retoucher les splits déjà présents.

#### Lecture directe sans NFS (`--direct-read`) et espace scratch (`--spill-dir`)

Pour supprimer totalement l'intermédiaire NFS (objectif day4 §2), démarrez le master avec
`--crawl <CRAWL_ID>` (il embarque l'URL WET dans chaque tâche MAP) et lancez les workers
avec `--direct-read` : chaque split est streamé puis décompressé **directement en mémoire**
depuis `data.commoncrawl.org` (S3/HTTPS), sans fichier NFS ni staging disque.

Le `/tmp` par défaut peut être un `tmpfs` réduit qui sature sous des centaines de
téléchargements concurrents. [src/benchmarks/find_scratch.sh](src/benchmarks/find_scratch.sh) inspecte
les montages (`df`/`mount`), exclut le NFS et recommande la plus grande partition locale
inscriptible ; pointez-y le worker via `--spill-dir` (staging) et `-l` (partitions MAP) :

```bash
ssh <node> 'bash -s' < src/benchmarks/find_scratch.sh        # recommande un répertoire scratch
python3 src/mapreduce/worker.py -h <master> -p 54321 \
    -i <input> -o <output> -l <scratch>/map-outputs \
    --direct-read --spill-dir <scratch>
```

### 5.5 Cycle d'exécution MapReduce

1. Worker envoie READY_FOR_TASK.
2. Master assigne MAP, REDUCE ou WAIT.
3. Worker exécute la tâche.
4. Worker envoie TASK_FINISHED.
5. Master répond ACK.
6. En parallèle, chaque worker envoie un HEARTBEAT toutes les 2 s; le master
   détecte une panne sur fermeture TCP ou expiration du bail (60 s) et réassigne
   les tâches perdues (voir section 10).

Note: les workers n'ont pas besoin d'un script d'arrêt dédié dans ce workflow. Ils se terminent automatiquement lorsque le master ferme sa connexion (ou si le master est tué).

### 5.6 Détails de traitement

- MAP:
  - Lecture d'un split commun depuis NFS.
  - Tokenisation simple (mots alphanumériques en lowercase).
  - Partitionnement par reducer via crc32(word) % n_reducers.
  - Écriture intermédiaire locale dans /tmp.

- SHUFFLE:
  - Un reducer va chercher sa partition chez chaque map worker via SSH et cat.

- REDUCE:
  - Agrégation en mémoire des couples (mot, count).
  - Tri par fréquence décroissante.
  - Écriture finale dans le dossier output partagé.

## 6) Chemins de données et stockage

Variables utilisées dans [src/deploy/deploy_commoncrawl.sh](src/deploy/deploy_commoncrawl.sh)
(`$USER` = votre login Telecom) :

- NFS_DIR: ~/slr207-group1-commoncrawl-$USER
- NFS_INPUT_DIR: ~/slr207-group1-commoncrawl-$USER/input
- NFS_OUTPUT_DIR: ~/slr207-group1-commoncrawl-$USER/output
- LOCAL_MAP_DIR: /tmp/slr207-group1-commoncrawl-$USER/map-outputs
- REMOTE_LOG_DIR: /tmp/slr207-group1-commoncrawl-$USER/logs/<timestamp>

Modèle de stockage:

- Input CommonCrawl: NFS partagé.
- Intermédiaires MAP: disque local /tmp de chaque worker.
- Output REDUCE: NFS partagé.
- Logs: /tmp distant, par run timestamp.

## 7) Protocole de messages MapReduce

Transport:

- TCP socket.
- Messages JSON délimités par fin de ligne \n.

Worker vers Master:

- {"status": "READY_FOR_TASK"}
- {"status": "TASK_FINISHED"}
- {"status": "HEARTBEAT"}  (toutes les 2 s, détecteur de panne)

Master vers Worker:

- {"type": "MAP", "split_id": i, "n_reducers": r, "job": J, "url": ...}
- {"type": "WAIT"}
- {"type": "REDUCE", "reducer_id": j, "map_workers": [...], "job": J}
- {"status": "ACK"}

Note:

- Le déploiement CommonCrawl actif s'appuie sur [src/mapreduce](src/mapreduce).

## 8) Observabilité et logs

Logging applicatif:

- Master et worker loggent avec timestamp et niveau.
- Le déploiement CommonCrawl force python3 -u pour éviter la mise en buffer des logs.

Monitoring en live:

- [src/deploy/deploy_commoncrawl.sh](src/deploy/deploy_commoncrawl.sh) suit les logs master via tail -f.
- Le suivi s'arrête automatiquement à la fin du process master.

Conseil pratique:

- En cas d'échec partiel, récupérer les logs master et worker du dossier REMOTE_LOG_DIR du run.

## 9) Performance et bench

Outils disponibles:

- [src/benchmarks/bench.sh](src/benchmarks/bench.sh): chrono de phases shell (déploiement, upload, bootstrap, etc.).
- [src/benchmarks/bench.py](src/benchmarks/bench.py): utilitaire Python pour instrumentation complémentaire.

Mesures utiles:

1. Temps total de déploiement.
2. Temps de préparation des splits CommonCrawl.
3. Durée MAP.
4. Durée SHUFFLE+REDUCE.
5. Bottleneck (réseau, SSH, NFS, machine lente).

### 9.1 Sweep Amdahl sur le cluster

Le balayage Amdahl multi-nœuds (dataset fixe, N = 1, 2, 4, 8, 16 workers indépendants)
est déjà mesuré et livré (`report/amdahl_cluster.json` + `report/amdahl_cluster.png`).
Le reproduire (~15-20 min) :

```bash
bash src/benchmarks/amdahl_cluster_sweep.sh   # 16 splits, 8 reducers, N=1,2,4,8,16
python3 src/benchmarks/plot_amdahl.py \
    --input runtime/amdahl_cluster.json --output runtime/amdahl_cluster.png
```

Résultats mesurés: speedup jusqu'à **7,74×** à N=16, fraction série **f ≈ 0,065**
(plafond théorique ≈ 15,4×). Détails et figure dans [report/final_report.pdf](report/final_report.pdf).

## 10) Tolérance aux pannes

Le protocole V2 (voir [doc/fault_tolerance/fault_tolerance.seqdiag.txt](doc/fault_tolerance/fault_tolerance.seqdiag.txt))
ajoute, au-dessus du chemin nominal:

- **Détecteur de panne**: heartbeat worker -> master toutes les 2 s, bail `recv()`
  de 60 s côté master. Un process tué ferme sa socket (détection immédiate), une
  partition réseau est détectée à l'expiration du bail.
- **Réassignation** (papier MapReduce §3.1): tâche en cours -> remise en file;
  MAP terminé sur un worker mort -> **re-exécuté** (son `/tmp` est perdu);
  REDUCE terminé -> **conservé** (écrit atomiquement sur NFS).
- **Écriture atomique**: le reducer écrit `part-j.<pid>.tmp` puis `os.replace()`
  vers `part-j.txt` (exactly-once, jamais de fichier à moitié écrit).
- **Stragglers** (§3.6): quand la file MAP est vide mais que des tâches tournent
  encore, un worker libre reçoit un **doublon** du split le plus lent; le premier
  terminé gagne.

Démonstration (tuer un worker en plein job):

```bash
# terminal 1 : lancer un job
bash src/deploy/deploy_commoncrawl.sh -w 8 -r 8 -s 20
# terminal 2 : injecter une panne
bash src/deploy/fault_tolerance_demo.sh -n 1
```

Options de [src/deploy/fault_tolerance_demo.sh](src/deploy/fault_tolerance_demo.sh): `-n` nombre de workers à tuer, `-p` port, `-d` délai avant kill.

## 11) Use cases (jobs) au-delà du wordcount

Le même moteur distribué (même shuffle/reduce) sert quatre analyses; seul le keying
de la phase MAP change, sélectionné par `-j`:

| `-j` | Analyse | Clé MAP |
|------|---------|---------|
| `wordcount` (défaut) | fréquence des mots | mot -> 1 |
| `lang` | popularité des langues | hit de stop-word -> langue |
| `wordlen` | distribution des longueurs de mots | longueur -> 1 |
| `bigram` | popularité des phrases | paire de mots consécutifs -> 1 |

```bash
bash src/deploy/deploy_commoncrawl.sh -w 8 -r 8 -s 20 -j bigram
```

## 12) Validation des résultats

[src/mapreduce/validate.py](src/mapreduce/validate.py) recalcule le résultat sur
**une seule machine** à partir des mêmes splits et le compare à la sortie distribuée
(clés distinctes, totaux, égalité par clé). Il importe la même fonction de
tokenisation que le worker, garantissant un calcul de référence identique.

```bash
python3 src/mapreduce/validate.py -i <dossier_input> -o <dossier_output> -j wordcount
```

Sur le cluster, on lance la validation **sur la machine du master** (qui voit `input/` et
`output/` via le NFS partagé). `-s` doit valoir le **même nombre de splits** que le
déploiement (§5.2), sinon la référence ne correspond pas à la sortie :

```bash
scp src/mapreduce/validate.py tp-XXXX:~/slr207-group1-commoncrawl-$USER/
ssh tp-XXXX "python3 ~/slr207-group1-commoncrawl-$USER/validate.py \
    -i ~/slr207-group1-commoncrawl-$USER/input \
    -o ~/slr207-group1-commoncrawl-$USER/output -j wordcount -s 8"
```

**Exemple de sortie** (se termine par `✅ PASS`) :

```text
[VALIDATE] job=wordcount
[VALIDATE] distinct keys : reference=... distributed=... (match)
[VALIDATE] total counts  : reference=... distributed=... (match)
[VALIDATE] ✅ PASS — distributed output matches single-machine reference
```

Code de sortie: 0 = PASS, 1 = FAIL, 2 = erreur.

## 13) Nettoyage

```bash
bash src/deploy/kill_commoncrawl.sh            # tue master/workers + nettoie /tmp
bash src/deploy/kill_commoncrawl.sh -p 60000   # port spécifique
bash src/deploy/kill_commoncrawl.sh -d         # supprime aussi la sortie NFS
```

[src/deploy/kill_commoncrawl.sh](src/deploy/kill_commoncrawl.sh) est idempotent: il tue les
process master/worker, supprime les intermédiaires `/tmp` et les sockets de contrôle
SSH, en parallèle sur toutes les machines de `machines.txt`.

## 14) Comparaison Kafka Streams

Broker Kafka 4.3 single-node en mode KRaft, **sans Docker ni root**, installé sous
`/tmp/<user>-kafka`:

```bash
bash src/kafka/deploy_kafka.sh           # télécharge + démarre le broker
bash src/kafka/run_wordcount.sh <fichier> # démo Kafka Streams WordCount
bash src/kafka/clean_kafka.sh            # arrêt + nettoyage (-a wipe complet)
```

### Connecteur source Common Crawl → Kafka (§8)

Pour lire Common Crawl **directement** dans Kafka, sans NFS ni fichier local, le
connecteur source [src/kafka/commoncrawl_source.sh](src/kafka/commoncrawl_source.sh)
résout un split depuis `wet.paths.gz` puis streame + décompresse + filtre les en-têtes WARC
à la volée vers le topic d'entrée. Il est câblé dans la démo :

```bash
bash src/kafka/deploy_kafka.sh
bash src/kafka/run_wordcount.sh --crawl CC-MAIN-2024-10 --index 0   # source directe S3
# ou en autonome :
bash src/kafka/commoncrawl_source.sh -c CC-MAIN-2024-10 -i 0
```

Les comptes finaux coïncident avec ceux de notre moteur batch; la différence est
opérationnelle (stream continu vs résultat final unique). Détails et tableau
comparatif (nous vs Hadoop vs Kafka Streams): [report/final_report.pdf](report/final_report.pdf).

## 15) Livrables

- Rapport (7 points demandés): [report/final_report.pdf](report/final_report.pdf)
- Graphe de la loi d'Amdahl + données: [report/amdahl_speedup.png](report/amdahl_speedup.png), [report/amdahl_results.json](report/amdahl_results.json)
- Support de démo (10 min, format Marp): [slides/presentation.md](slides/presentation.md)
- Auto-évaluation remplie: [self-assessment.md](self-assessment.md)
- Test solo reproductible (sans cluster): [tests/run_all.py](tests/run_all.py)

## 16) Limites et choix techniques

- Le parsing CommonCrawl est volontairement simple pour rester léger (petite pollution de métadonnées possible dans les splits).
- La robustesse réseau reste dépendante de la stabilité SSH/Wi-Fi du lab.
- La tolérance aux pannes couvre la mort des **workers** (détection, ré-exécution, écriture atomique, stragglers). La **reprise du master** sur checkpoint est conçue dans le diagramme de séquence mais **non implémentée** dans le code.
- Si un worker source d'un MAP meurt **pendant** la phase REDUCE, le master revient en phase MAP et ré-exécute tous les reduces (correct mais coûteux).
- La tolérance aux pannes se teste **en solo** via [tests/run_all.py](tests/run_all.py) (shuffle en lecture disque locale, sans `sshd`). Le chemin de shuffle **distant par `ssh cat`** ne s'exerce, lui, que sur le cluster Linux.
