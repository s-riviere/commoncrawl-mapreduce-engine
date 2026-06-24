# TESTING.md — Runbook de démo / test (à copier-coller le jour du contrôle)

> **But de ce document.** Il contient **toutes** les commandes, dans l'ordre, pour
> démontrer et tester le projet **sur les machines de TP de Telecom Paris** (la cible
> réelle). Chaque commande est **expliquée en détail** et **prête au copier-coller**.
> Tout est calibré pour une démo de **2-3 min** (chemin rapide en §0), avec des sections
> détaillées ensuite si vous avez plus de temps.
>
> **Règle d'or de la démo :** une seule chose change d'une machine à l'autre, c'est
> `tp-XXXX` = le nom de la machine du **master** (affiché par le script au démarrage).
> `$USER` se remplit tout seul (votre login Telecom). Ne remplacez **que** les `<...>`.

---

## Table des matières

- [0. Démo express (2-3 min) — le chemin à suivre le jour J](#0-démo-express-2-3-min--le-chemin-à-suivre-le-jour-j)
- [1. Modèle d'exécution (cluster vs solo)](#1-modèle-dexécution-cluster-vs-solo)
- [2. Prérequis et conventions](#2-prérequis-et-conventions)
- [3. Vérifier l'accès SSH + la liste de machines](#3-vérifier-laccès-ssh--la-liste-de-machines)
- [4. Workflow A — CPU Load (échauffement réseau, optionnel)](#4-workflow-a--cpu-load-échauffement-réseau-optionnel)
- [5. Workflow B — MapReduce sur Common Crawl](#5-workflow-b--mapreduce-sur-common-crawl)
  - [5.1 Déploiement + monitoring](#51-déploiement--monitoring)
  - [5.2 Lire la sortie](#52-lire-la-sortie)
  - [5.3 Validation de correction](#53-validation-de-correction)
  - [5.4 Démo de tolérance aux pannes](#54-démo-de-tolérance-aux-pannes)
  - [5.5 Les 4 analyses (use cases)](#55-les-4-analyses-use-cases)
- [6. Loi d'Amdahl sur le cluster (résultats réels)](#6-loi-damdahl-sur-le-cluster-résultats-réels)
- [7. Comparaison Kafka Streams + connecteur source](#7-comparaison-kafka-streams--connecteur-source)
- [8. Nettoyage (ne rien laisser traîner)](#8-nettoyage-ne-rien-laisser-traîner)
- [9. Annexe — test solo sur une seule machine (sans cluster)](#9-annexe--test-solo-sur-une-seule-machine-sans-cluster)
- [10. Dépannage](#10-dépannage)
- [11. Checklist de validation](#11-checklist-de-validation)

---

## 0. Démo express (2-3 min) — le chemin à suivre le jour J

> Les **splits Common Crawl sont déjà présents sur le NFS** (`input/commoncrawl-00*.txt`),
> donc **aucun téléchargement** n'est nécessaire pendant la démo : le job démarre tout de
> suite. Toutes les commandes se lancent **depuis la racine du dépôt**, sur votre machine.

```bash
# (0) Se placer à la racine du dépôt (adapter le chemin).
cd ~/Distributed-Computing-Project

# (1) DÉPLOIEMENT + EXÉCUTION : 8 workers, 8 reducers, 8 splits, wordcount.
#     demo_verified_run.sh lit une liste de machines DÉJÀ vérifiées joignables
#     (runtime/machines.verified.txt), évite les machines bloquées, démarre le
#     master puis les workers, et STREAME le log master jusqu'à la fin du job.
#     Le job se termine en ~30-60 s. Notez le nom du master « tp-XXXX » affiché
#     en Phase 1 (« Master etabli sur tp-XXXX:54321 »).
bash src/benchmarks/demo_verified_run.sh -w 8 -r 8 -s 8 -j wordcount

# (2) VALIDATION : prouve que la sortie distribuée == calcul mono-machine.
#     Remplacer tp-XXXX par le master affiché à l'étape (1).
#     IMPORTANT : -s doit valoir le MÊME nombre de splits que l'étape (1) (ici 8),
#     sinon la référence ne correspond pas à la sortie. Le recalcul est séquentiel
#     sur UNE machine : comptez ~4 min pour 8 splits (c'est normal, pas un blocage).
scp src/mapreduce/validate.py tp-XXXX:~/slr207-group1-commoncrawl-$USER/
ssh tp-XXXX "python3 ~/slr207-group1-commoncrawl-$USER/validate.py \
    -i ~/slr207-group1-commoncrawl-$USER/input \
    -o ~/slr207-group1-commoncrawl-$USER/output -j wordcount -s 8"
# Attendu : se termine par « ✅ PASS » (code de sortie 0).

# (3) TOLÉRANCE AUX PANNES : relancer un job, puis tuer un worker en plein vol.
#     Terminal A : lancer un job un peu plus gros pour avoir le temps.
bash src/benchmarks/demo_verified_run.sh -w 8 -r 8 -s 16 -j wordcount
#     Terminal B (PENDANT que le job tourne) : tuer 1 worker.
bash src/deploy/fault_tolerance_demo.sh -n 1
#     Dans le log master (terminal A) on voit : worker perdu détecté → MAP réassigné
#     → le job se termine quand même correctement.

# (4) NETTOYAGE (garde les splits pour pouvoir re-démontrer) :
bash src/deploy/kill_commoncrawl.sh
```

C'est tout pour la démo cœur. Les sections suivantes détaillent chaque étape, ajoutent
Amdahl (déjà mesuré, voir §6), Kafka (§7) et le wipe complet (§8).

---

## 1. Modèle d'exécution (cluster vs solo)

| Mode | Quand l'utiliser | Lance quoi |
|------|------------------|------------|
| **Cluster Telecom** (cible) | Évaluation, démo, mesures Amdahl réelles | 1 *master* + N *workers* sur N+1 machines distinctes, via SSH |
| **Solo une machine** (annexe §9) | Valider le code sans accès cluster | master + N workers en processus locaux sur `localhost` |

Le **même moteur** (`master.py` / `worker.py`) tourne dans les deux cas. Seule différence :
en solo, le shuffle lit les partitions sur disque local (`--local-shuffle`) au lieu de les
tirer par `ssh cat` entre machines.

```
            ┌──────────── machine 1 : MASTER (master.py) ─────────────┐
            │  charge les tâches, distribue MAP puis REDUCE, détecte   │
            │  les pannes (heartbeat / lease), écrit le TIMING final   │
            └──────────────▲──────────────▲──────────────▲────────────┘
                TCP/JSON    │              │              │
        ┌───────────────────┘    ┌─────────┘    ┌─────────┘
   machine 2 : WORKER        machine 3 : WORKER  ...  machine N+1 : WORKER
   MAP → /tmp/.../partition_j   idem                    idem
   REDUCE ← ssh cat des partitions de tous les workers → part-j.txt (NFS, atomique)
```

---

## 2. Prérequis et conventions

1. **Compte Telecom Paris actif** avec accès SSH aux machines de TP (`tp-*.enst.fr`).
2. **Réseau campus** (Wi-Fi `campus telecom`, **pas** `eduroam`) ou VPN/passerelle de l'école.
3. **Clés SSH** configurées pour un accès **non interactif** (sans mot de passe).
4. **Python 3.10+** sur les machines (déjà présent en TP). `numpy` est un accélérateur
   **optionnel** (repli Python pur automatique s'il manque) — aucune dépendance obligatoire
   côté cluster.
5. Outils shell locaux : `ssh`, `scp`, `timeout`, `curl`, `jq`.
6. **Aucun privilège root** requis : tout vit dans le HOME NFS et `/tmp` local.

### 2.1 Conventions de notation (à lire une fois)

| Notation | Ce que c'est | À remplacer ? |
|----------|--------------|---------------|
| `~` | votre dossier personnel (« home ») | non, le shell s'en charge |
| `$USER` | **rempli automatiquement** par Linux avec votre login Telecom | non, tapez la commande telle quelle |
| `$(hostname)` | remplacé par le nom de la machine courante | non |
| `<...>` ou `tp-XXXX` | un **trou à remplir vous-même** (login, nom de machine…) | **oui** |

```bash
echo "$USER"                          # affiche votre login (ce que $USER vaut chez vous)
ls -d ~/slr207-group1-commoncrawl-*   # retrouve le nom exact du dossier de travail partagé
```

> **Pourquoi `$USER` dans les chemins ?** Les scripts créent un dossier de travail dont le
> nom contient votre login (`~/slr207-group1-commoncrawl-$USER`) pour que deux membres du
> groupe ne se marchent pas dessus. Comme `$USER` est identique sur votre machine et sur les
> machines Telecom (même compte), les commandes se copient-collent sans rien changer.

---

## 3. Vérifier l'accès SSH + la liste de machines

```bash
# Vérifier qu'une connexion SSH passe SANS mot de passe (remplacez le nom de machine).
ssh tp-1a201-25.enst.fr "echo OK && hostname"
# Attendu : « OK » puis « tp-1a201-25 ».
```

Deux façons d'obtenir la liste des machines :

- **Recommandé pour la démo : liste vérifiée** `runtime/machines.verified.txt` (machines
  testées joignables au préalable). C'est elle qu'utilise `demo_verified_run.sh`. Elle
  évite de tomber sur une machine bloquée pendant le contrôle.

  ```bash
  wc -l runtime/machines.verified.txt   # nb de machines disponibles
  head -5 runtime/machines.verified.txt # aperçu
  ```

- **Alternative : génération live via l'API** (`src/deploy/get_machines.sh`, qui interroge
  `https://tp.telecom-paris.fr/ajax.php` et garde les machines vivantes et libres). C'est ce
  qu'utilise `deploy_commoncrawl.sh`. Plus « propre » mais dépend de l'API au moment T.

  ```bash
  bash src/deploy/get_machines.sh -n 21 > runtime/machines.txt   # 21 = 1 master + 20 workers
  wc -l runtime/machines.txt
  ```

> `runtime/` est **gitignored** : `machines.txt`, les logs et les artefacts y sont des
> sous-produits non versionnés.

---

## 4. Workflow A — CPU Load (échauffement réseau, optionnel)

Petit serveur TCP déployé sur N machines qui renvoie les *load averages* (1/5/15 min) ;
un client les interroge en parallèle. Utile pour montrer que le déploiement SSH + NFS marche
avant de lancer MapReduce.

```bash
# Déploie un serveur par machine (port 54321 par défaut). Génère runtime/machines.txt
# via l'API, uploade src/server/server.py sur le NFS, démarre les serveurs.
bash src/deploy/deploy_cpuload.sh

# Interroge tous les nœuds en parallèle et affiche les moyennes cluster 1/5/15 min.
python3 src/client/client.py

# Arrêt + nettoyage : tue le process du port et supprime les dossiers distants utilisés.
bash src/deploy/kill_cpuload.sh
```

Variantes (port personnalisé, synchro d'horloge) :

```bash
bash src/deploy/deploy_cpuload.sh 54555                 # autre port
python3 src/client/client.py 54555 --sync tp-XXXX       # tp-XXXX = machine horloge de référence
bash src/deploy/kill_cpuload.sh 54555
```

---

## 5. Workflow B — MapReduce sur Common Crawl

### 5.1 Déploiement + monitoring

**Une seule commande** déploie tout : sélection des machines (liste vérifiée), upload sur le
NFS, téléchargement des splits **manquants seulement**, démarrage du master puis des workers,
et **streaming en direct du log master** jusqu'à la fin du job.

```bash
# Démo fiable (liste vérifiée, pas d'appel API) : 8 workers, 8 reducers, 8 splits, wordcount.
bash src/benchmarks/demo_verified_run.sh -w 8 -r 8 -s 8 -j wordcount
```

Options de `demo_verified_run.sh` :

| Flag | Sens | Défaut |
|------|------|--------|
| `-p` | port du master | `54321` |
| `-w` | nombre de **workers** (machines) | `4` |
| `-r` | nombre de **reducers** | `4` |
| `-s` | nombre de **splits** Common Crawl (télécharge seulement les manquants) | `8` |
| `-j` | analyse : `wordcount` \| `lang` \| `wordlen` \| `bigram` | `wordcount` |
| `-m` | liste de machines vérifiées | `runtime/machines.verified.txt` |

Les 4 phases affichées :

```
 Phase 0 : Using VERIFIED machine list (no API)
 Phase 1 : NFS Upload & Master Bootstrap      ← affiche « Master etabli sur tp-XXXX:54321 »
 Phase 2 : Workers Bootstrap (sequential)
 Phase 3 : Monitoring MapReduce Execution     ← streame le log master, sort à la fin du job
```

**Emplacements des fichiers** (`$USER` = votre login) :

- Entrée (NFS **partagé**) : `~/slr207-group1-commoncrawl-$USER/input/commoncrawl-*.txt`
- Sortie (NFS **partagé**) : `~/slr207-group1-commoncrawl-$USER/output/part-*.txt`
- Partitions MAP intermédiaires (disque **local** `/tmp`) :
  `/tmp/slr207-group1-commoncrawl-$USER/map-outputs/`
- Logs (local `/tmp`) : `/tmp/slr207-group1-commoncrawl-$USER/logs/<date>/master_*.log`, `worker_*.log`

> La dernière ligne du log master est le **TIMING** structuré, exploitable pour Amdahl :
> `TIMING: {"t_total": ..., "t_map": ..., "t_reduce": ...}`.
>
> `Ctrl+C` pendant la Phase 3 **détache** seulement l'affichage ; le job **continue** sur le
> cluster.

> **Alternative API** (sans liste vérifiée), strictement équivalente côté NFS/process :
> `bash src/deploy/deploy_commoncrawl.sh -p 54321 -w 8 -r 8 -s 8 -j wordcount`.

### 5.2 Lire la sortie

```bash
# Remplacer tp-XXXX par le master. Top 20 des mots les plus fréquents :
ssh tp-XXXX "cat ~/slr207-group1-commoncrawl-$USER/output/part-*.txt | sort -t$'\t' -k2 -nr | head -20"
```

Chaque ligne `part-j.txt` est `clé<TAB>compte`, triée par fréquence décroissante.

### 5.3 Validation de correction

La sortie distribuée est comparée à un **calcul mono-machine de référence**
([src/mapreduce/validate.py](src/mapreduce/validate.py)) : mêmes clés distinctes, même total,
égalité clé par clé. `validate.py` n'est pas copié par le déploiement : on le copie puis on le
lance **sur la machine du master** (qui voit déjà `input/` et `output/` via le NFS partagé).

> **Seule chose à remplacer :** `tp-XXXX` = la machine du master (Phase 1).
>
> **Important :** `-s` doit valoir le **même nombre de splits** que le déploiement §5.1
> (ici `8`), sinon la référence ne correspond pas à la sortie. Le recalcul de référence
> est **séquentiel sur une seule machine** : comptez **~4 min pour 8 splits** (≈ 8 min
> pour 16). C'est normal — ce n'est pas un blocage.

```bash
# (1) Copier l'outil de validation dans le dossier de travail partagé (NFS).
scp src/mapreduce/validate.py tp-XXXX:~/slr207-group1-commoncrawl-$USER/

# (2) Lancer la validation SUR le master, via ssh.
#     -i = dossier d'entrée (référence)  -o = sortie distribuée  -j = analyse testée
#     -s = nombre de splits validés (DOIT être identique au -s du déploiement §5.1)
ssh tp-XXXX "python3 ~/slr207-group1-commoncrawl-$USER/validate.py \
    -i ~/slr207-group1-commoncrawl-$USER/input \
    -o ~/slr207-group1-commoncrawl-$USER/output \
    -j wordcount -s 8"
```

Sortie attendue (se termine par le `✅ PASS`, code de sortie `0`) :

```text
[VALIDATE] job=wordcount
[VALIDATE] reference   ← .../input
[VALIDATE] distributed ← .../output
[VALIDATE] distinct keys : reference=... distributed=... (match)
[VALIDATE] total counts  : reference=... distributed=... (match)
[VALIDATE] ✅ PASS — distributed output matches single-machine reference
```

Code de sortie : `0` = PASS, `1` = FAIL, `2` = erreur.

### 5.4 Démo de tolérance aux pannes

On tue un worker **pendant** un job ; le master détecte la perte (fermeture TCP / bail de
heartbeat de 60 s), **remet en file** la tâche en cours, **ré-exécute** les MAP perdus (leur
`/tmp` est parti) et **conserve** les REDUCE déjà écrits (écriture atomique `.tmp` + `os.replace`).

```bash
# Terminal A : lancer un job assez gros pour avoir le temps d'injecter la panne.
bash src/benchmarks/demo_verified_run.sh -w 8 -r 8 -s 16 -j wordcount

# Terminal B (PENDANT que le job tourne) : tuer 1 worker maintenant.
bash src/deploy/fault_tolerance_demo.sh -n 1
```

Options de `fault_tolerance_demo.sh` : `-n` nombre de workers à tuer, `-p` port, `-d` délai
(en s) avant de tuer. Exemple : `bash src/deploy/fault_tolerance_demo.sh -n 2 -d 5`.

Dans le log master (terminal A), vous verrez successivement : un worker détecté mort, les
tâches MAP correspondantes **réassignées**, puis le job qui **se termine correctement**. On peut
re-valider avec §5.3 pour prouver que le résultat reste exact malgré la panne.

### 5.5 Les 4 analyses (use cases)

Même moteur (même shuffle/reduce) ; seul le *keying* de la phase MAP change, via `-j` :

| `-j` | Analyse | Clé MAP |
|------|---------|---------|
| `wordcount` (défaut) | fréquence des mots | mot → 1 |
| `lang` | langues dominantes | hit d'un stop-word → langue |
| `wordlen` | distribution des longueurs de mots | longueur → 1 |
| `bigram` | popularité des paires de mots | paire de mots consécutifs → 1 |

```bash
bash src/benchmarks/demo_verified_run.sh -w 8 -r 8 -s 8 -j lang      # langues dominantes
bash src/benchmarks/demo_verified_run.sh -w 8 -r 8 -s 8 -j wordlen   # longueurs de mots
bash src/benchmarks/demo_verified_run.sh -w 8 -r 8 -s 8 -j bigram    # paires de mots
```

Chaque analyse se valide de la même façon (changer `-j` dans la commande §5.3).

---

## 6. Loi d'Amdahl sur le cluster (résultats réels)

Le balayage a **déjà été mesuré sur le cluster** et est livré dans le rapport. Inutile de le
refaire pour la démo — montrez le graphe et le tableau.

**Résultats réels** (16 splits fixes, 8 reducers, N workers indépendants) :

| N (workers) | t_total (s) | speedup |
|-------------|-------------|---------|
| 1 | 521.66 | 1.00× |
| 2 | 262.80 | 1.99× |
| 4 | 137.15 | 3.80× |
| 8 | 84.60 | 6.17× |
| 16 | 67.37 | 7.74× |

Fit d'Amdahl : fraction série **f ≈ 0,065** (≈ 93,5 % parallélisable), plafond théorique
**≈ 15,4×**. Artefacts livrés : [report/amdahl_cluster.png](report/amdahl_cluster.png),
[report/amdahl_cluster.json](report/amdahl_cluster.json).

**Reproduire** (si demandé — c'est long, ~15-20 min) :

```bash
# Balaye N=1,2,4,8,16 sur 16 splits et écrit runtime/amdahl_cluster.json.
bash src/benchmarks/amdahl_cluster_sweep.sh
# Rend la figure (matplotlib requis ; un venv suffit).
python3 src/benchmarks/plot_amdahl.py --input runtime/amdahl_cluster.json --output runtime/amdahl_cluster.png
```

---

## 7. Comparaison Kafka Streams + connecteur source

Broker Kafka single-node en mode KRaft, **sans Docker ni root**, installé sous
`/tmp/$USER-kafka`. Sert à comparer batch (notre moteur) vs stream (Kafka : « jamais terminé »).

```bash
# (1) Télécharge (si absent) + démarre le broker Kafka en arrière-plan.
bash src/kafka/deploy_kafka.sh

# (2a) WordCount Kafka Streams sur un fichier local (un split déjà sur NFS, p. ex.) :
bash src/kafka/run_wordcount.sh ~/slr207-group1-commoncrawl-$USER/input/commoncrawl-0000.txt

# (2b) OU lecture DIRECTE depuis Common Crawl S3/HTTPS (connecteur source, sans NFS ni fichier) :
bash src/kafka/run_wordcount.sh --crawl CC-MAIN-2024-10 --index 0

# (3) Nettoyage Kafka (voir §8).
bash src/kafka/clean_kafka.sh        # garde les binaires (re-run rapide)
```

> Le connecteur source [src/kafka/commoncrawl_source.sh](src/kafka/commoncrawl_source.sh)
> résout un split depuis `wet.paths.gz`, le streame + décompresse + filtre les en-têtes WARC
> à la volée vers le topic d'entrée. Les comptes finaux coïncident avec ceux de notre moteur
> batch ; la différence est opérationnelle (flux continu vs résultat final unique).

---

## 8. Nettoyage (ne rien laisser traîner)

> Les scripts de nettoyage ne touchent **que vos propres processus** (`pkill -u $(id -u)`) :
> ils ne tueront jamais le `master.py`/`worker.py` d'un autre étudiant sur une machine
> partagée. SIGTERM puis SIGKILL, avec timeouts SSH pour ne pas bloquer sur une machine lente.

**MapReduce — garder les splits (recommandé pendant la démo, pour re-démontrer vite) :**

```bash
# Tue master + workers (UID-scoped, TERM puis KILL), nettoie /tmp + sockets SSH de contrôle.
# NE supprime PAS le dossier NFS → les splits input/ restent en place (pas de re-téléchargement).
bash src/deploy/kill_commoncrawl.sh
```

**MapReduce — wipe complet (à la toute fin) :**

```bash
# Idem + supprime le dossier NFS (input + output + logs). À utiliser quand tout est fini.
bash src/deploy/kill_commoncrawl.sh -d
# Restreindre à un port précis si plusieurs jobs : bash src/deploy/kill_commoncrawl.sh -p 54321
```

**CPU Load :**

```bash
bash src/deploy/kill_cpuload.sh            # ou: bash src/deploy/kill_cpuload.sh <port>
```

**Kafka :**

```bash
bash src/kafka/clean_kafka.sh              # stoppe broker + streams, supprime données/logs (garde les binaires)
bash src/kafka/clean_kafka.sh -a           # wipe complet : supprime aussi les binaires Kafka téléchargés
```

**Vérifier qu'il ne reste rien à nous (optionnel, sur une machine donnée) :**

```bash
ssh tp-XXXX "pgrep -u \$(id -u) -af '[m]aster\.py|[w]orker\.py|kafka\.Kafka' || echo 'aucun process à nous'"
```

> Le motif `[m]aster\.py` évite que la commande `pgrep` ne se matche elle-même.

---

## 9. Annexe — test solo sur une seule machine (sans cluster)

Pas besoin du cluster ni de l'équipe pour valider le **code** : un cluster MapReduce complet
(master + N workers en processus locaux, shuffle en lecture disque locale, **aucun `sshd`**)
tourne sur votre seule machine.

```bash
# Suite complète : correction des 4 analyses + tolérance aux pannes + Amdahl (9/9 checks).
python3 tests/run_all.py
python3 tests/run_all.py --quick   # version rapide (datasets réduits)
python3 tests/run_all.py --keep    # conserve les fichiers /tmp générés

# Démo manuelle pas-à-pas (master + N workers à la main) sur un dossier de splits :
bash src/benchmarks/local_cluster.sh -i <dossier_splits> -n 4 --validate
```

Code de sortie `0` si les 9 vérifications passent (utilisable en CI). Le même moteur
(`master.py`/`worker.py`) tourne en solo et en multi-nœuds.

> Figure Amdahl en solo : `tests/run_all.py` (re)génère `runtime/amdahl_results.json` et la
> figure `runtime/amdahl_speedup.png` (matplotlib requis ; un venv suffit).

---

## 10. Dépannage

| Symptôme | Cause probable | Remède |
|----------|----------------|--------|
| `Permission denied (publickey)` au SSH | clé non chargée / mauvais login | vérifier `~/.ssh/config` (§2) ; `ssh-add ~/.ssh/<clé>` |
| `FATAL: liste de machines verifiees introuvable` | `runtime/machines.verified.txt` absent | utiliser l'alternative API (§3) ou régénérer la liste vérifiée |
| Le déploiement tombe sur des machines `[FAILED]` | machines bloquées/occupées | `demo_verified_run.sh` enchaîne automatiquement ; sinon augmenter la liste vérifiée |
| Port déjà utilisé | un master précédent tourne encore | `bash src/deploy/kill_commoncrawl.sh -p <port>` |
| `/tmp` plein sous forte charge | `/tmp` est un petit `tmpfs` | `ssh tp-NODE "bash -s" < src/benchmarks/find_scratch.sh` puis worker `--spill-dir` |
| Processus qui « restent » après coup | nettoyage incomplet | `bash src/deploy/kill_commoncrawl.sh` (UID-scoped, TERM+KILL) ; vérifier avec la commande §8 |
| Kafka : broker encore là | teardown partiel | `bash src/kafka/clean_kafka.sh -a` (wipe complet) |

---

## 11. Checklist de validation

- [ ] SSH non interactif OK vers au moins une machine `tp-*` (§3).
- [ ] Déploiement MapReduce → job terminé, `TIMING` affiché (§5.1).
- [ ] Sortie lisible `part-*.txt` triée par fréquence (§5.2).
- [ ] `validate.py` → `✅ PASS` (§5.3).
- [ ] Tolérance aux pannes : worker tué en vol → job termine correctement (§5.4).
- [ ] Les 4 analyses (`wordcount`/`lang`/`wordlen`/`bigram`) exécutables (§5.5).
- [ ] Amdahl : graphe + tableau présentés (§6).
- [ ] Kafka : WordCount + connecteur source direct (§7).
- [ ] Nettoyage : aucun process à nous restant, dossiers nettoyés (§8).
