# Guide de test complet — MapReduce distribué sur les machines Telecom

> **À lire par l'équipe.** Ce document décrit, pas à pas, **comment tester le projet
> sur les machines de TP de Telecom Paris** (la cible réelle du projet), avec les
> commandes exactes et des **exemples de sortie réels**. Un mode « solo une machine »
> est fourni en annexe pour valider le code sans le cluster, mais **le projet s'évalue
> sur le cluster**.

## Table des matières

1. [Modèle d'exécution (cluster vs solo)](#1-modèle-dexécution-cluster-vs-solo)
2. [Prérequis](#2-prérequis)
3. [Préparation de l'accès SSH](#3-préparation-de-laccès-ssh)
4. [Workflow A — CPU Load (échauffement réseau)](#4-workflow-a--cpu-load-échauffement-réseau)
5. [Workflow B — MapReduce sur Common Crawl](#5-workflow-b--mapreduce-sur-common-crawl)
   - [5.1 Déploiement + monitoring](#51-déploiement--monitoring)
   - [5.2 Exemple de sortie réel](#52-exemple-de-sortie-réel)
   - [5.3 Validation de correction](#53-validation-de-correction)
   - [5.4 Démo de tolérance aux pannes](#54-démo-de-tolérance-aux-pannes)
   - [5.5 Les 4 analyses (use cases)](#55-les-4-analyses-use-cases)
   - [5.6 Passage à l'échelle (100 / 1000 splits)](#56-passage-à-léchelle-100--1000-splits)
   - [5.7 Lecture directe sans NFS + scratch](#57-lecture-directe-sans-nfs--scratch)
6. [Loi d'Amdahl sur le cluster](#6-loi-damdahl-sur-le-cluster)
7. [Comparaison Kafka Streams + connecteur source](#7-comparaison-kafka-streams--connecteur-source)
8. [Nettoyage](#8-nettoyage)
9. [Annexe — test solo sur une seule machine](#9-annexe--test-solo-sur-une-seule-machine)
10. [Dépannage](#10-dépannage)
11. [Checklist de validation](#11-checklist-de-validation)

---

## 1. Modèle d'exécution (cluster vs solo)

| Mode | Quand l'utiliser | Lance quoi |
|------|------------------|------------|
| **Cluster Telecom** (cible) | Évaluation, démo, mesures Amdahl réelles | 1 *master* + N *workers* sur N+1 machines distinctes via SSH |
| **Solo une machine** (annexe) | Valider le code sans accès cluster | master + N workers en processus locaux sur `localhost` |

Le **même moteur** (`master.py` / `worker.py`) tourne dans les deux cas. La seule
différence : en solo, le shuffle lit les partitions sur disque local
(`--local-shuffle`) au lieu de les tirer par `ssh cat` entre machines.

Architecture (cluster) :

```
            ┌───────────── machine 1 : MASTER (master.py) ─────────────┐
            │  charge les tâches, distribue MAP puis REDUCE, détecte    │
            │  les pannes (heartbeat/lease), écrit le TIMING final      │
            └───────────────▲───────────────▲───────────────▲──────────┘
                 TCP/JSON    │               │               │
        ┌───────────────────┘     ┌─────────┘     ┌─────────┘
   machine 2 : WORKER         machine 3 : WORKER   ...   machine N+1 : WORKER
   MAP → /tmp/.../partition_j   idem                       idem
   REDUCE ← ssh cat des partitions de tous les workers → part-j.txt (NFS, atomique)
```

---

## 2. Prérequis

1. **Compte Telecom Paris actif** avec accès SSH aux machines de TP (`tp-*.enst.fr`).
2. **Réseau campus** : être sur le Wi-Fi `campus telecom` (PAS `eduroam`, qui n'est pas
   sur le même réseau que les machines de l'école), ou connecté via le VPN/gateway de
   l'école.
3. **Clés SSH** configurées pour un accès **non interactif** (sans mot de passe).
4. **Python 3.10+** sur les machines (déjà présent sur les machines de TP).
5. Outils shell locaux : `ssh`, `scp`, `timeout`, `curl`, `jq`.
6. **Aucun privilège root** n'est requis (tout vit dans le HOME NFS et `/tmp` local).

Le code n'a **aucune dépendance Python obligatoire** côté cluster : `numpy` est un
accélérateur optionnel (repli Python pur automatique s'il est absent).

### 2.1 Conventions de notation (à lire avant de commencer)

Toutes les commandes de ce guide sont prévues pour un terminal **Linux / bash** ordinaire
(c'est celui des machines de TP et de toute distribution Linux). Quatre notations
reviennent ; comprenez-les une fois et tout le reste se lit sans surprise :

| Notation | Ce que c'est | Faut-il la remplacer ? |
|----------|--------------|------------------------|
| `~` | votre dossier personnel (« home »), p. ex. `/home/jdupont` | non, le shell s'en charge |
| `$USER` | variable **remplie automatiquement** par Linux avec votre identifiant de connexion (votre login Telecom) | non, tapez la commande telle quelle |
| `$(hostname)` | remplacé automatiquement par le nom de la machine où la commande s'exécute | non |
| `<quelque-chose>` ou `tp-XXXX` | un **trou à remplir vous-même** (ce n'est PAS une variable) : un login, un nom de machine, etc. | **oui**, remplacez-le |

Deux réflexes utiles :

```bash
echo "$USER"                              # affiche votre login (ce que $USER vaut chez vous)
ls -d ~/slr207-group1-commoncrawl-*       # retrouve le nom exact du dossier de travail partagé
```

> **Pourquoi `$USER` apparaît dans les chemins ?** Les scripts créent un dossier de travail
> dont le nom contient votre login pour que deux personnes du même groupe ne se marchent
> pas dessus. Ce dossier s'appelle **toujours** `~/slr207-group1-commoncrawl-$USER` ; comme
> `$USER` est identique sur votre machine et sur les machines Telecom (même compte), vous
> pouvez copier-coller les commandes sans rien changer.

---

## 3. Préparation de l'accès SSH

Ajoutez ceci à `~/.ssh/config` (remplacez `<login>` et le nom de clé) :

```sshconfig
Host tp-*
  User <login>
  PreferredAuthentications publickey
  IdentityFile ~/.ssh/<votre_cle>
  StrictHostKeyChecking no
```

Vérifiez qu'une connexion passe sans mot de passe :

```bash
ssh tp-1a201-02.enst.fr "echo OK && hostname"
# Attendu : OK  puis  tp-1a201-02
```

> Si l'accès direct échoue mais qu'une passerelle est disponible, ajoutez
> `ProxyJump <gateway>` dans le bloc `Host tp-*`.

La **liste des machines** est générée automatiquement par les scripts de déploiement
(via [scripts/get_machines.sh](../scripts/get_machines.sh), qui interroge l'API
`https://tp.telecom-paris.fr/ajax.php` et garde les machines **vivantes et libres**).
Elle est écrite dans `runtime/machines.txt`. Pour la générer manuellement :

```bash
bash scripts/get_machines.sh -n 21 > runtime/machines.txt   # 21 = 1 master + 20 workers
wc -l runtime/machines.txt
```

---

## 4. Workflow A — CPU Load (échauffement réseau)

But : valider que le déploiement parallèle SSH et la collecte réseau fonctionnent.

```bash
# 1) Déployer un serveur de charge sur toutes les machines (port 54321 par défaut)
bash scripts/deploy_cpuload.sh

# 2) Interroger le cluster en parallèle (charge moyenne 1/5/15 min)
python3 src/cpu_load/client.py

# 3) Nettoyer
bash scripts/kill_cpuload.sh
```

Options utiles :

```bash
bash scripts/deploy_cpuload.sh 54555            # port personnalisé
python3 src/cpu_load/client.py 54555 --sync tp-XXXX   # tp-XXXX = machine servant d'horloge de référence
bash scripts/kill_cpuload.sh 54555
```

Le client affiche les nœuds joignables / non joignables puis les moyennes agrégées du
cluster sur 1, 5 et 15 minutes.

---

## 5. Workflow B — MapReduce sur Common Crawl

### 5.1 Déploiement + monitoring

Une seule commande déploie **tout** : sélection des machines, upload sur le NFS,
téléchargement des splits manquants, démarrage du master puis des workers, et **streaming
en direct du log master** jusqu'à la fin du job.

```bash
# 8 workers, 8 reducers, 20 splits Common Crawl, job wordcount
bash scripts/deploy_commoncrawl.sh -p 54321 -w 8 -r 8 -s 20 -j wordcount
```

Paramètres :

| Flag | Sens | Défaut |
|------|------|--------|
| `-p` | port du master | `54321` |
| `-w` | nombre de **workers** (machines) | `10` |
| `-r` | nombre de **reducers** | `10` |
| `-s` | nombre de **splits** Common Crawl (télécharge seulement les manquants) | `10` |
| `-j` | analyse : `wordcount` \| `lang` \| `wordlen` \| `bigram` | `wordcount` |

Ce que fait le script, en 4 phases (affichées à l'écran) :

```
 Phase 0 : Fetching Alive Machines from API
 Phase 1 : NFS Upload & Master Bootstrap
 Phase 2 : Workers Bootstrap
 Phase 3 : Monitoring MapReduce Execution
```

- **Emplacements** des fichiers (rappel : `$USER` = votre login, voir §2.1) :
  - Entrée (dossier **partagé** par NFS, visible depuis toutes les machines) :
    `~/slr207-group1-commoncrawl-$USER/input/commoncrawl-*.txt`
  - Sortie (dossier **partagé**) : `~/slr207-group1-commoncrawl-$USER/output/part-*.txt`
  - Partitions MAP intermédiaires (disque **local** à chaque machine, dans `/tmp`) :
    `/tmp/slr207-group1-commoncrawl-$USER/map-outputs/`
  - Logs (local, dans `/tmp`) : `/tmp/slr207-group1-commoncrawl-$USER/logs/<date-heure>/master_*.log` et `worker_*.log`

  > « NFS » = système de fichiers réseau : votre dossier personnel est le **même** sur
  > toutes les machines de TP. Écrire un fichier depuis une machine le rend lisible par
  > toutes les autres — c'est ce qui permet au master et aux workers de partager l'entrée
  > et la sortie.

- `Ctrl+C` pendant la phase 3 **détache** seulement l'affichage du log ; le job, lui,
  **continue** de tourner sur le cluster en arrière-plan.

### 5.2 Exemple de sortie réel

Extrait **authentique** du log master pendant un job (format identique sur le cluster ;
seuls les `worker-id` deviennent des noms de machines `tp-…` au lieu de `w0/w1/…`) :

```text
[2026-06-23 22:49:09] [MASTER] [INFO] Tasks loaded: 4 MAP, 3 REDUCE (job=wordcount)
[2026-06-23 22:49:09] [MASTER] [INFO] Worker registered: id=w0 host=127.0.0.1 map_dir=/tmp/mr-local-user/map/w0
[2026-06-23 22:49:09] [MASTER] [INFO] w2 finished MAP 2 (3/4)
[2026-06-23 22:49:09] [MASTER] [INFO] w1 finished MAP 3 (4/4)
[2026-06-23 22:49:09] [MASTER] [INFO] MAP phase complete; switching to REDUCE (sources=2)
[2026-06-23 22:49:09] [MASTER] [INFO] w0 starts REDUCE 0
[2026-06-23 22:49:09] [MASTER] [INFO] w0 finished REDUCE 0 (1/3)
[2026-06-23 22:49:09] [MASTER] [INFO] w1 finished REDUCE 1 (2/3)
[2026-06-23 22:49:09] [MASTER] [INFO] w0 finished REDUCE 2 (3/3)
[2026-06-23 22:49:09] [MASTER] [INFO] REDUCE phase complete; job finished
TIMING: {"t_total": 0.420, "t_map": 0.415, "t_reduce": 0.005}
```

Côté worker, chaque phase émet une ligne de chronométrage structurée (`WORKER_TIMING`)
exploitable pour le profilage :

```text
[2026-06-23 22:49:09] [WORKER] [INFO] MAP start split=3 reducers=3 input=.../commoncrawl-0003.txt
[2026-06-23 22:49:09] [WORKER] [INFO] MAP done split=3 t_download=0.00s t_clean=0.00s t_io_read=0.00s t_compute=0.00s t_io_write=0.00s t_total=0.00s unique_words=20 tokens=3120
WORKER_TIMING: {"phase":"MAP","split_id":3,"t_download":0.000,"t_clean":0.001,"t_io_read":0.000,"t_compute":0.001,"t_io_write":0.000}
[2026-06-23 22:49:09] [WORKER] [INFO] REDUCE start reducer=0 sources=2 local_shuffle=True
WORKER_TIMING: {"phase":"REDUCE","reducer_id":0,"t_shuffle":0.001,"t_compute":0.000,"t_io_write":0.000}
```

> Sur le cluster, `local_shuffle=False` : le reducer tire les partitions des autres
> workers via `ssh cat` (shuffle distant) au lieu de les lire sur le disque local.

### 5.3 Validation de correction

La sortie distribuée est comparée à un **calcul mono-machine de référence**
([src/map_reduce/validate.py](../src/map_reduce/validate.py)) : mêmes clés distinctes,
même total, et égalité clé par clé.

`validate.py` n'est pas copié automatiquement par le déploiement. La marche à suivre tient
en deux gestes : (1) copier l'outil dans le dossier de travail partagé, (2) le lancer sur la
machine du master (comme le dossier personnel est partagé par NFS, cette machine voit déjà
les sous-dossiers `input/` et `output/`).

**Une seule chose à remplacer** : `tp-XXXX`, par le nom de la machine où le master a démarré
(ce nom est affiché pendant la **Phase 1** du déploiement, à la ligne « Master Bootstrap »).

```bash
# (1) Copier l'outil de validation dans le dossier de travail partagé.
#     ~/slr207-group1-commoncrawl-$USER est ce dossier ; $USER se remplit tout seul (§2.1).
scp src/map_reduce/validate.py tp-XXXX:~/slr207-group1-commoncrawl-$USER/

# (2) Lancer la validation SUR la machine du master, via ssh.
#     -i = dossier d'entrée (référence)   -o = dossier de sortie distribuée   -j = analyse testée
ssh tp-XXXX "python3 ~/slr207-group1-commoncrawl-$USER/validate.py \
    -i ~/slr207-group1-commoncrawl-$USER/input \
    -o ~/slr207-group1-commoncrawl-$USER/output \
    -j wordcount"
```

> Le `\` en fin de ligne signifie simplement « la commande continue à la ligne suivante » :
> vous pouvez aussi tout écrire sur une seule ligne. Les guillemets autour du `python3 …`
> servent à envoyer toute la commande à la machine distante en un bloc.

Sortie **réelle** (capturée en exécution) — le `✅ PASS` est la preuve de correction :

```text
[VALIDATE] job=wordcount
[VALIDATE] reference  ← /tmp/doc-demo
[VALIDATE] distributed ← /tmp/mr-local-user/out
[VALIDATE] splits read = 4 | reducer files = 3
[VALIDATE] distinct keys : reference=20  distributed=20
[VALIDATE] total counts  : reference=12,480  distributed=12,480

[VALIDATE] Top entries (reference):
         2,400  the
           960  cat
           960  hello
           480  quick
           480  brown
           480  fox
           480  jumps
           480  over
           480  lazy
           480  dog

[VALIDATE] ✅ PASS — distributed output matches the single-machine reference exactly.
```

Code de sortie : `0` = PASS, `1` = différences détectées, `2` = entrée/sortie introuvable.

### 5.4 Démo de tolérance aux pannes

Dans un **second terminal**, quelques secondes après le début du job, tuez un (ou
plusieurs) worker(s) pour simuler une panne :

```bash
bash scripts/fault_tolerance_demo.sh -n 1        # tue 1 worker maintenant
bash scripts/fault_tolerance_demo.sh -n 2 -d 5   # attend 5 s, puis tue 2 workers
```

Le script ne tue **jamais** le master (il saute la 1re machine de la liste). Dans le log
master, vous devez voir la détection + la ré-exécution (format exact, issu du code) :

```text
[MASTER] [WARN] tp-1a207-13.enst.fr DIED holding 2 completed MAP output(s) → re-running [5, 9]
[MASTER] [INFO] tp-1a201-05.enst.fr starts MAP 5
[MASTER] [INFO] ...
[MASTER] [INFO] REDUCE phase complete; job finished
```

Règles de ré-exécution appliquées (Google MapReduce §3.1) :
- tâche **en cours** sur un mort → re-mise en file ;
- **MAP terminé** sur un mort → **re-exécuté** (sa sortie `/tmp` est perdue avec la machine) ;
- **REDUCE terminé** → **conservé** (écrit atomiquement sur le NFS) ;
- si une **source MAP** meurt **pendant** la phase REDUCE → retour en phase MAP, ré-exécution
  des maps perdus, puis re-mise en file de tous les REDUCE (sûr car la sortie reduce est atomique).

Après la fin, **rejouez la validation** (§5.3) : elle doit toujours afficher `✅ PASS`.

### 5.5 Les 4 analyses (use cases)

Le même pipeline supporte 4 analyses via `-j`. Elles partagent shuffle + reduce et ne
diffèrent que par la **clé émise en MAP** :

```bash
bash scripts/deploy_commoncrawl.sh -w 8 -r 8 -s 20 -j wordcount   # fréquence des mots
bash scripts/deploy_commoncrawl.sh -w 8 -r 8 -s 20 -j lang        # langues dominantes (stop-words)
bash scripts/deploy_commoncrawl.sh -w 8 -r 8 -s 20 -j wordlen     # distribution des longueurs de mots
bash scripts/deploy_commoncrawl.sh -w 8 -r 8 -s 20 -j bigram      # popularité des paires de mots
```

Validez chacune en passant le **même** `-j` à `validate.py`.

### 5.6 Passage à l'échelle (100 / 1000 splits)

Stress-test demandé par l'énoncé (day4 §3). Augmentez `-s` (et `-w`/`-r`) :

```bash
bash scripts/deploy_commoncrawl.sh -w 20 -r 20 -s 100  -j wordcount
bash scripts/deploy_commoncrawl.sh -w 30 -r 30 -s 1000 -j wordcount
```

Le master distribue `-s` tâches MAP sur `-w` workers ; comme `#splits ≫ #workers`, la
charge s'équilibre naturellement et les **stragglers** déclenchent des tâches *backup*.
Surveillez le `TIMING` final et les `WORKER_TIMING` pour repérer les goulots.

### 5.7 Lecture directe sans NFS + scratch

**Lecture directe Common Crawl (day4 §2)** — supprime totalement l'intermédiaire NFS :
démarrez le master avec `--crawl <ID>` (il embarque l'URL de téléchargement dans chaque
tâche MAP) et les workers avec `--direct-read` (téléchargement + décompression **en
mémoire**, sans écrire de fichier). Le déploiement standard n'expose pas encore ces options ;
pour un test ciblé, on lance le master puis les workers à la main.

À remplacer : `tp-MASTER` (machine choisie pour le master) et `tp-WORKER` (chacune des
machines workers). Le reste se remplit tout seul.

```bash
# Sur la machine du master :
ssh tp-MASTER "python3 ~/slr207-group1-commoncrawl-$USER/master.py \
    -p 54321 -r 8 -s 20 -j wordcount --crawl CC-MAIN-2024-10"

# Sur CHAQUE machine worker (répétez en changeant tp-WORKER) :
ssh tp-WORKER "python3 ~/slr207-group1-commoncrawl-$USER/worker.py \
    -h tp-MASTER -p 54321 \
    -i ~/slr207-group1-commoncrawl-$USER/input \
    -o ~/slr207-group1-commoncrawl-$USER/output \
    -l /tmp/mapreduce-$USER \
    --worker-id \$(hostname) --direct-read"
```

> `--worker-id \$(hostname)` : l'antislash devant `$(hostname)` fait que le nom est calculé
> **sur la machine worker** (et non sur la vôtre). Chaque worker reçoit ainsi un identifiant
> unique égal à son propre nom de machine.

**Espace « scratch » (day4 §2, au-delà de `/tmp`)** — sur certaines machines `/tmp` est un
petit disque en mémoire (`tmpfs`) qui peut se remplir quand des centaines de téléchargements
arrivent en même temps. Le script [scripts/find_scratch.sh](../scripts/find_scratch.sh)
inspecte les disques de la machine, ignore le NFS, et recommande la plus grande partition
**locale** où l'on a le droit d'écrire.

```bash
# Demander à une machine quel disque local utiliser (remplacez tp-NODE par son nom) :
ssh tp-NODE "bash -s" < scripts/find_scratch.sh
```

Le script affiche un dossier conseillé (par exemple `/local/scratch`). Il suffit alors
d'ajouter deux options à la commande du worker ci-dessus — en remplaçant le chemin par
celui qu'il a conseillé :

```text
    --spill-dir /local/scratch  -l /local/scratch/map-outputs
```

---

## 6. Loi d'Amdahl sur le cluster

Le balayage multi-nœuds est dans [amdahl_bench.py](../amdahl_bench.py) : il déploie des
workers sur N machines réelles pour N = 1, 2, 4, 8, 16, 32, lance le job sur **un dataset
fixe**, parse les lignes `TIMING:` / `WORKER_TIMING:` et trace le speedup.

```bash
python3 amdahl_bench.py            # voir --help pour les options (workers, splits, port)
python3 plot_amdahl.py            # rend la figure depuis le JSON de résultats
```

Règle de validité (impérative) : **le jeu de données doit être identique pour tous les
points** d'une même courbe, et le point de référence est `S(1) = 1`. Avec ≥ 2 nœuds on
**mesure** (jamais d'extrapolation).

> Le graphe et les données livrés avec le rapport
> ([report/amdahl_speedup.png](../report/amdahl_speedup.png),
> [report/amdahl_results.json](../report/amdahl_results.json)) proviennent du mode
> reproductible (annexe §9). Sur le cluster, chaque worker possède sa propre bande
> passante mémoire : la fraction série baisse et le plafond monte.

---

## 7. Comparaison Kafka Streams + connecteur source

Broker Kafka 4.3 mono-nœud en mode **KRaft**, **sans Docker ni privilèges root**. Il
s'installe dans `/tmp/<votre-login>-kafka` (le `<votre-login>` est rempli automatiquement
par les scripts) :

```bash
bash scripts/kafka/deploy_kafka.sh                         # télécharge + démarre le broker
bash scripts/kafka/run_wordcount.sh <fichier.txt>          # démo WordCount sur un fichier
bash scripts/kafka/run_wordcount.sh --crawl CC-MAIN-2024-10 --index 0   # source S3 directe
bash scripts/kafka/clean_kafka.sh                          # arrêt + nettoyage (-a wipe)
```

**Connecteur source Common Crawl → Kafka** (§8) :
[scripts/kafka/commoncrawl_source.sh](../scripts/kafka/commoncrawl_source.sh) résout un
split depuis `wet.paths.gz`, puis **streame + décompresse + filtre les en-têtes WARC à la
volée** vers le topic d'entrée — **sans NFS ni fichier local**.

```bash
bash scripts/kafka/commoncrawl_source.sh -c CC-MAIN-2024-10 -i 0
```

---

## 8. Nettoyage

Toujours nettoyer avant de redéployer (le script est idempotent) :

```bash
bash scripts/kill_commoncrawl.sh             # tue master+workers, nettoie /tmp + sockets SSH
bash scripts/kill_commoncrawl.sh -p 54321    # restreint à un port
bash scripts/kill_commoncrawl.sh -d          # + supprime le répertoire NFS (input/output/logs)
```

Pour CPU Load : `bash scripts/kill_cpuload.sh [port]`.
Pour Kafka : `bash scripts/kafka/clean_kafka.sh [-a]`.

---

## 9. Annexe — test solo sur une seule machine

Pour valider le code **sans cluster** (utile quand l'accès Telecom n'est pas disponible),
le harnais [tests/run_all.py](../tests/run_all.py) lance un cluster MapReduce complet en
processus locaux sur `localhost` (shuffle en lecture disque locale, **aucun `sshd`**) :

```bash
python3 tests/run_all.py            # suite complète : 9/9 vérifications
python3 tests/run_all.py --quick    # version rapide (datasets réduits)
```

Il couvre : correction des 4 analyses (vs référence), tolérance aux pannes (kill d'un
worker en cours de job), et 3 points Amdahl (N = 1, 2, 4). Code de sortie `0` si tout
passe.

Démo manuelle pas-à-pas sur une machine :

```bash
bash scripts/local_cluster.sh -i <dossier_splits> -n 4 -r 4 -j wordcount --validate
```

> **Limite assumée** : le mode solo n'exerce **pas** le shuffle distant par `ssh cat`
> (il lit les partitions sur disque local). Ce chemin réseau ne se teste que sur le
> cluster (§5).

---

## 10. Dépannage

| Symptôme | Cause probable | Action |
|----------|----------------|--------|
| `Permission denied (publickey)` | clé SSH absente des `authorized_keys` de la machine, ou compte « exterieurs » sans login direct | vérifier `ssh tp-… "echo OK"` ; ajouter `ProxyJump <gateway>` ; régénérer/copier la clé |
| `FATAL: Not enough machines` | l'API a renvoyé moins de N+1 machines libres | réessayer plus tard, baisser `-w`, ou regénérer `runtime/machines.txt` |
| `Could not initialize master` | toutes les machines testées injoignables | vérifier réseau campus (pas eduroam), réessayer |
| Job bloqué en phase MAP | un split manquant et pas de `--crawl` pour le télécharger | vérifier `~/slr207-group1-commoncrawl-$USER/input/` ; relancer le déploiement (télécharge les manquants) |
| `validate.py: no part-*.txt found` | le job n'a pas (encore) écrit la sortie, ou mauvais répertoire | attendre la fin du job ; vérifier le chemin `output/` |
| Port déjà utilisé | un master précédent tourne encore | `bash scripts/kill_commoncrawl.sh -p <port>` |
| `/tmp` plein sous forte charge | `/tmp` est un petit `tmpfs` | utiliser `scripts/find_scratch.sh` + `--spill-dir` (§5.7) |

---

## 11. Checklist de validation

- [ ] `ssh tp-… "echo OK"` passe sans mot de passe.
- [ ] **CPU Load** : déploiement + client + kill OK (§4).
- [ ] **MapReduce wordcount** déployé, le log master atteint `REDUCE phase complete; job finished` + `TIMING:` (§5.1/5.2).
- [ ] **Validation** `✅ PASS` pour `wordcount` (§5.3).
- [ ] **Tolérance aux pannes** : worker tué, log `… DIED … → re-running`, job fini, validation toujours `✅ PASS` (§5.4).
- [ ] Les **4 analyses** (`wordcount`/`lang`/`wordlen`/`bigram`) validées (§5.5).
- [ ] **Passage à l'échelle** : un run à 100 splits (et idéalement 1000) terminé (§5.6).
- [ ] **Amdahl** : balayage multi-nœuds tracé, `S(1)=1`, dataset fixe (§6).
- [ ] **Kafka** : broker déployé, WordCount OK, connecteur source Common Crawl OK (§7).
- [ ] **Nettoyage** effectué (`kill_commoncrawl.sh -d`) (§8).
