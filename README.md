# MapReduce — Challenge 1 : charge CPU distribuée

## Description

Déploiement d'un serveur de charge CPU sur les machines des salles TP de
Télécom Paris, collecte de la charge (`loadavg`) de chaque nœud depuis un
client, et calcul de la charge moyenne du cluster.

## Configuration initiale

A la racine du fichier, tapez la commande suivante pour setup pyproject :
```
pip install -e .
```

Dans `~/.ssh/config`, ajoutez ceci :
```
Host tp-*
  User rdeloye-24 // A remplacer par votre identifiant
  PreferredAuthentications publickey
  IdentityFile "~/.ssh/telecom_paris" // A remplacer par votre clé
```

## Architecture

```
  get_machines.py  ──HTTP──► https://tp.telecom-paris.fr/ajax.php
                             filtre les machines alive et libres
      │
      ▼
  machines.txt               (top 50 machines les moins chargées)
      │
      ▼
  deploy.sh  ──scp──► 1 machine TP : upload server.py (NFS ~/),
                       machines.txt vers /tmp/slr207-group1/
             ──ssh (parallèle)──► toutes les machines TP,
                                   lance nohup python3 ~/server.py
      │
      ▼
  (déploiement terminé, hostname de la machine /tmp affiché)
      │
      ▼
  client.py  ──scp --sync──► récupère machines.txt depuis /tmp/slr207-group1/
             ──TCP séquentiel──► tous les serveurs vivants
                                lit /proc/loadavg, affiche par nœud +
                                moyennes globales (1, 5, 15 min)
      │
      ▼
  kill.sh    ──ssh──► machines TP : fuser + kill PID sur le port,
                      supprime server.py et .server.pid de /tmp/slr207-group1/
```

Le code serveur est sur le **home NFS** (`~/server.py`), visible depuis toutes
les machines. Seul `machines.txt` est stocké dans `/tmp/slr207-group1/` (disque
local d'une machine).

## Fichiers

| Fichier | Rôle |
|---------|------|
| `get_machines.py` | Interroge l'API Télécom Paris, sélectionne les 50 machines les plus libres, écrit `machines.txt` |
| `machines.txt` | Liste dynamique des machines cibles (générée automatiquement) |
| `server.py` | Écoute TCP sur le port choisi, envoie `load1 load5 load15` |
| `client.py` | Interroge en parallèle toutes les machines, affiche les stats. Supporte `--sync` pour récupérer `machines.txt` depuis `/tmp` |
| `deploy.sh` | Phase 0 : appelle `get_machines.py`. Phase 1 : upload `server.py` via NFS (`~/`) et `machines.txt` vers `/tmp/slr207-group1/` sur une machine. Phase 2 : lance le serveur en parallèle sur toutes les machines |
| `kill.sh` | Trouve le PID via `fuser <port>/tcp`, envoie SIGTERM puis SIGKILL, nettoie `/tmp/slr207-group1/` |
| `.editorconfig` | Force les fins de ligne LF pour éviter les problèmes CRLF sous Windows/WSL |

## Pré-requis

1. **Compte Télécom Paris** actif.
2. **Connexion au Wi-Fi campus** (nécessaire pour l'API et l'accès SSH direct).
3. **Clé SSH** copiée sur les machines TP :
   ```bash
   ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
   ssh-copy-id -i ~/.ssh/id_ed25519.pub <login>@tp-1a201-01.enst.fr
   ```

## Utilisation

### Déployer (une seule personne du groupe)
```bash
./deploy.sh           # port 54321 par défaut
./deploy.sh 54555     # port personnalisé
```

Sortie typique :
```
=================================================
 Phase 0: Fetching Alive Machines from API
=================================================
  → 50 machines loaded from machines.txt

=================================================
 Phase 1: Parallel Deployment
=================================================
[1/2] Syncing server.py + machines.txt to local disk on a lab machine...
      Trying tp-1a201-04.enst.fr                ... [OK]
[2/2] Bootstrapping cluster processes...
  [STARTED] -> tp-1a201-04.enst.fr
  [STARTED] -> tp-1a201-07.enst.fr
  ...
=================================================
 Deployment complete.

 machines.txt stored on: tp-1a201-04.enst.fr:/tmp/slr207-group1/

 Verify:      python3 client.py 54321
 Team sync:   python3 client.py 54321 --sync tp-1a201-04.enst.fr
=================================================
```

### Interroger le cluster

**Depuis la machine du déployeur :**
```bash
python3 client.py              # utilise le machines.txt local + port 54321
python3 client.py 54555        # port personnalisé
```

**Depuis la machine d'un autre membre (sync) :**
```bash
python3 client.py 54321 --sync tp-1a201-04.enst.fr
```

Cela récupère `machines.txt` depuis `/tmp/slr207-group1/` de la machine
indiquée, puis interroge tous les serveurs.

Sortie :
```
Connecting to 50 machines on port 54321...

  tp-1a201-04.enst.fr         load: 0.00  0.00  0.00
  tp-1a201-07.enst.fr         load: 0.02  0.01  0.00
  ...
=======================================================
  Nodes responded: 48/50
  Avg cluster load  1 min : 0.0124
  Avg cluster load  5 min : 0.0098
  Avg cluster load 15 min : 0.0067
=======================================================
```

### Arrêter et nettoyer
```bash
./kill.sh
```

- Trouve le PID écoutant sur le port via `fuser`, puis envoie SIGTERM + SIGKILL.
- Supprime `server.py` et `.server.pid` de `/tmp/slr207-group1/` sur chaque
  machine.

## Protocole

Très simple : 1 connexion TCP = 1 réponse.

```
client  ─── TCP connect ────► serveur
client  ◄── "l1 l5 l15\n" ─── serveur
        (connexion fermée côté serveur)
```

- `l1`, `l5`, `l15` : floats, charge moyenne sur 1 / 5 / 15 minutes
  (lus depuis `/proc/loadavg`, exactement comme `uptime`).
- Port par défaut : **54321** (plage haute, peu susceptible de conflit).

## Choix techniques / bonnes pratiques

- **Stockage hybride** : `server.py` sur le home NFS (`~/`), visible depuis
  toutes les machines. `machines.txt` sur le disque local (`/tmp/slr207-group1/`)
  d'une machine de référence.
- **Génération dynamique** de `machines.txt` via l'API : pas de liste statique
  à maintenir, détecte automatiquement les machines allumées et libres.
- **Port élevé** (`54321`) : pas de conflit avec services système (< 1024) ni
  ports classiques.
- **`SO_REUSEADDR`** : permet de relancer le serveur rapidement après kill.
- **Déploiement parallèle** : lance les serveurs sur toutes les machines en
  parallèle via des sous-shells SSH.
- **`--sync` dans le client** : permet à tout membre du groupe de récupérer
  `machines.txt` depuis `/tmp` d'une machine lab sans avoir à re-déployer.
- **IPv4 forcé** (`-4`) : évite les bugs de résolution IPv6 sous WSL2.
- **`BatchMode=yes` + `ConnectTimeout`** : les scripts échouent vite sur une
  machine injoignable au lieu de bloquer.
- **Côté client, interrogation séquentielle** avec délai de 0.5s entre chaque
  requête pour éviter le rate-limiting SSH côté serveur.
- **`.editorconfig`** : force LF pour éviter les problèmes CRLF qui cassent
  les scripts shell sous WSL/Linux.

## Respect de l'infrastructure partagée

- Le serveur est **minuscule** (quasi idle). Il ne consomme rien : juste un
  `accept()` bloquant.
- **Pas d'orchestrateur central** ni de nœud maître — simple pattern
  client/serveur sans état.
- **Cleanup systématique** via `./kill.sh` : ne laisse aucun processus
  orphelin ni fichier sur les machines.
- **Limite raisonnable** (50 machines) ; pas de scan agressif du parc.

## Limitations connues

- Nécessite une connexion au Wi-Fi campus pour accéder à l'API et aux
  machines.
- Serveur mono-thread : `accept()` séquentiel suffit pour le protocole
  (quelques connexions courtes).
- Pas de chiffrement applicatif : la charge CPU n'est pas sensible, et le
  trafic transite sur le réseau interne ENST.
