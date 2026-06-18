# Distributed Computing Project (SLR)

Projet de calcul distribué sur les machines de TP Telecom Paris avec deux workflows principaux:

1. CPU Load: déploiement d'un serveur TCP minimal sur plusieurs noeuds, puis interrogation parallèle des load averages.
2. CommonCrawl + MapReduce: pipeline MAP/SHUFFLE/REDUCE distribué pour compter les mots sur des splits CommonCrawl.

Ce README décrit l'état actuel du dépôt et les scripts utilisés en pratique.

## Table des matières

- [Distributed Computing Project (SLR)](#distributed-computing-project-slr)
  - [Table des matières](#table-des-matières)
  - [1) Vue d'ensemble](#1-vue-densemble)
  - [2) Prérequis](#2-prérequis)
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
  - [10) Limites et choix techniques](#10-limites-et-choix-techniques)

## 1) Vue d'ensemble

Le projet est organisé autour de scripts shell qui automatisent le déploiement sur des machines distantes accessibles en SSH.

- Sélection des machines: [scripts/get_machines.sh](scripts/get_machines.sh)
- Déploiement CPU Load: [scripts/deploy_cpuload.sh](scripts/deploy_cpuload.sh)
- Arrêt/cleanup CPU Load: [scripts/kill_cpuload.sh](scripts/kill_cpuload.sh)
- Déploiement CommonCrawl + MapReduce: [scripts/deploy_commoncrawl.sh](scripts/deploy_commoncrawl.sh)
- Outils de bench shell: [scripts/bench.sh](scripts/bench.sh)

Le pipeline MapReduce actif est dans [src/map_reduce](src/map_reduce): master/worker et utilitaires CommonCrawl.

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

## 3) Structure du dépôt

```text
scripts/
  deploy_cpuload.sh         # déploiement parallèle CPU Load
  kill_cpuload.sh           # arrêt + nettoyage distant CPU Load
  deploy_commoncrawl.sh     # déploiement complet CommonCrawl + MapReduce
  get_machines.sh           # sélection des meilleures machines via API
  bench.sh                  # instrumentation timing shell

src/
  cpu_load/
    server.py               # serveur TCP renvoyant load averages
    client.py               # client parallèle + rapport agrégat
  map_reduce/
    master.py               # master utilisé en déploiement CommonCrawl
    worker.py               # worker utilisé en déploiement CommonCrawl
    download_commoncrawl.py # téléchargement WET en splits texte

runtime/
  machines.txt              # liste des machines cibles

doc/
  fault_tolerance/
  map_reduce/
  guidelines/
```

## 4) Workflow A - CPU Load

### 4.1 Objectif

Démarrer un petit serveur TCP sur plusieurs machines et collecter la charge CPU moyenne (1/5/15 min) en parallèle.

### 4.2 Déploiement

```bash
bash scripts/deploy_cpuload.sh
bash scripts/deploy_cpuload.sh 54555
```

Le script:

1. Génère [runtime/machines.txt](runtime/machines.txt) via [scripts/get_machines.sh](scripts/get_machines.sh).
2. Uploade [src/cpu_load/server.py](src/cpu_load/server.py) sur NFS distant.
3. Démarre un serveur par machine en parallèle.

### 4.3 Interrogation du cluster

```bash
python3 src/cpu_load/client.py
python3 src/cpu_load/client.py 54555
python3 src/cpu_load/client.py 54555 --sync <host_reference>
```

Le client:

- Interroge les noeuds en parallèle (ThreadPoolExecutor).
- Affiche noeuds joignables/non joignables.
- Calcule les moyennes cluster sur 1/5/15 minutes.

### 4.4 Arrêt et nettoyage

```bash
bash scripts/kill_cpuload.sh
bash scripts/kill_cpuload.sh 54555
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
bash scripts/deploy_commoncrawl.sh
bash scripts/deploy_commoncrawl.sh -p 60000 -w 8 -r 8 -s 20
```

Paramètres principaux:

- -p: port master (défaut 54321)
- -w: nombre de workers (défaut 10)
- -r: nombre de reducers (défaut 10)
- -s: nombre de splits CommonCrawl cibles (défaut 10)

### 5.3 Ce que fait le script de déploiement

1. Récupère N+1 machines (1 master + N workers).
2. Uploade [src/map_reduce/master.py](src/map_reduce/master.py), [src/map_reduce/worker.py](src/map_reduce/worker.py), [src/map_reduce/download_commoncrawl.py](src/map_reduce/download_commoncrawl.py) vers NFS distant.
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

### 5.5 Cycle d'exécution MapReduce

1. Worker envoie READY_FOR_TASK.
2. Master assigne MAP, REDUCE ou WAIT.
3. Worker exécute la tâche.
4. Worker envoie TASK_FINISHED.
5. Master répond ACK.

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

Variables utilisées dans [scripts/deploy_commoncrawl.sh](scripts/deploy_commoncrawl.sh):

- NFS_DIR: ~/slr207-group1
- NFS_INPUT_DIR: ~/slr207-group1/input
- NFS_OUTPUT_DIR: ~/slr207-group1/output
- LOCAL_MAP_DIR: /tmp/slr207-group1/map-outputs
- REMOTE_LOG_DIR: /tmp/slr207-group1/logs/<timestamp>

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

Master vers Worker:

- {"type": "MAP", "split_id": i, "n_reducers": r}
- {"type": "WAIT"}
- {"type": "REDUCE", "reducer_id": j, "map_workers": [...]}
- {"status": "ACK"}

Note:

- Le déploiement CommonCrawl actif s'appuie sur [src/map_reduce](src/map_reduce).

## 8) Observabilité et logs

Logging applicatif:

- Master et worker loggent avec timestamp et niveau.
- Le déploiement CommonCrawl force python3 -u pour éviter la mise en buffer des logs.

Monitoring en live:

- [scripts/deploy_commoncrawl.sh](scripts/deploy_commoncrawl.sh) suit les logs master via tail -f.
- Le suivi s'arrête automatiquement à la fin du process master.

Conseil pratique:

- En cas d'échec partiel, récupérer les logs master et worker du dossier REMOTE_LOG_DIR du run.

## 9) Performance et bench

Outils disponibles:

- [scripts/bench.sh](scripts/bench.sh): chrono de phases shell (déploiement, upload, bootstrap, etc.).
- [src/common/bench.py](src/common/bench.py): utilitaire Python pour instrumentation complémentaire.

Mesures utiles:

1. Temps total de déploiement.
2. Temps de préparation des splits CommonCrawl.
3. Durée MAP.
4. Durée SHUFFLE+REDUCE.
5. Bottleneck (réseau, SSH, NFS, machine lente).

## 10) Limites et choix techniques

- Le parsing CommonCrawl est volontairement simple pour rester léger (petite pollution de métadonnées possible dans les splits).
- La robustesse réseau reste dépendante de la stabilité SSH/Wi-Fi du lab.
- Le protocole actuel ne couvre pas encore toutes les stratégies de tolérance aux pannes (timeout/retry/reassign complets). Si un des workers échoue durant l'éxecution d'un map reduce actuel, le processus entier sera bloqué et il faudra manuellement tuer master pour recommencer.
