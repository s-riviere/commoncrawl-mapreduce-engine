# PLEASE DO NOT RUN THIS AT ALL THERE IS ACCESS ISSUE WE DONT HAVE RIGHT TO USE JUMP HOST

# MapReduce — Challenge 1 : charge CPU distribuée

Déploiement d'un serveur de charge CPU sur les machines des salles TP de
Télécom Paris, collecte de la charge (`loadavg`) de chaque nœud depuis un
client, et calcul de la charge moyenne du cluster.

## Objectif du TP

- Fichier contenant les noms des machines `tp-*.enst.fr` (home NFS partagé).
- Script de déploiement : **1 `scp`** pour copier le serveur,
  **1 `ssh` par machine** pour le démarrer.
- Le serveur écoute sur un port spécifique (choisi haut pour éviter les
  conflits).
- Chaque membre du groupe peut lancer le client depuis sa propre machine et
  se connecter aux serveurs déployés.
- Protocole : connexion TCP → le serveur envoie `load1 load5 load15` (mêmes
  valeurs que `uptime`) → le client affiche la charge moyenne du cluster.

## Architecture

```
machines.txt                     (univers : tp-1a201-01 .. tp-1a201-40)
      │
      ▼
  deploy.sh  ──scp──►  jump host (ssh.enst.fr)  :  upload server.py + machines.txt via NFS
             ──ssh──►                            :  boucle séquentielle, ssh direct
                                                    vers chaque machine, lance nohup server
             ◄──scp──                            :  télécharge machines_alive.txt
      │
      ▼
machines_alive.txt               (liste dynamique des machines qui ont répondu)
      │
      ▼
  client.py  ──TCP parallèle──►  tous les serveurs vivants
                                 lit /proc/loadavg, affiche par nœud +
                                 moyennes globales (1, 5, 15 min)
      │
      ▼
  kill.sh    ──ssh──►  jump host  :  boucle, tue chaque serveur par PID
                                     (fichier ~/.server.pid)
                                     supprime ~/server.py du NFS
             nettoie localement : supprime machines_alive.txt
```

Le **home NFS** est partagé entre toutes les machines : un seul `scp` rend
`server.py` visible partout.

## Fichiers

| Fichier | Rôle |
|---------|------|
| `machines.txt` | Univers des machines (40 × `tp-1a201-XX.enst.fr`) |
| `server.py` | Écoute TCP sur le port choisi, envoie `load1 load5 load15` |
| `client.py` | Interroge en parallèle toutes les machines vivantes, affiche les stats |
| `deploy.sh` | 1 SCP + boucle SSH depuis le jump host ; génère `machines_alive.txt` |
| `kill.sh` | Tue les serveurs via leur PID, nettoie NFS et fichiers locaux |

## Pré-requis

Remplacer `<login>` ci-dessous par votre login Télécom Paris (ex. `prenom-25`).

1. **Compte Télécom Paris** actif.
2. **Clé SSH locale** copiée sur `ssh.enst.fr` :
   ```bash
   ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
   ssh-copy-id -i ~/.ssh/id_ed25519.pub <login>@ssh.enst.fr
   ```
3. **Clé SSH sur le jump host** (une fois), pour qu'il puisse se connecter
   aux machines de TP via le home NFS partagé :
   ```bash
   ssh <login>@ssh.enst.fr
   ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
   cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
   chmod 600 ~/.ssh/authorized_keys
   exit
   ```
4. **Config SSH locale** (`~/.ssh/config`) :
   ```
   Host telecom
       HostName ssh.enst.fr
       User <login>

   Host tp-*
       ProxyJump telecom
       User <login>
   ```
5. **Mettre à jour les scripts** : remplacer la valeur de `REMOTE_USER` en
   tête de `deploy.sh` et `kill.sh` par votre login.

> Une seule personne du groupe a besoin de faire le déploiement ; les autres
> membres n'ont qu'à récupérer `client.py` et `machines_alive.txt` pour
> interroger le cluster.

## Utilisation

### Déployer
```bash
./deploy.sh           # port 54321 par défaut
./deploy.sh 54555     # port personnalisé
```

Sortie typique :
```
[1/2] Uploading server.py and machines.txt via NFS...
[2/2] Starting server on each machine from jump host...
  [OK]  tp-1a201-01.enst.fr
  ...
  [--]  tp-1a201-35.enst.fr       <- machine éteinte
  ...
  --> 39 / 40 machines deployed.
Alive: 39 / 40  -->  machines_alive.txt
```

### Interroger le cluster (chaque membre du groupe depuis sa machine)
```bash
python3 client.py              # utilise machines_alive.txt + port 54321
python3 client.py 54555        # port personnalisé
```

Sortie :
```
Connecting to 39 machines on port 54321...

  tp-1a201-01.enst.fr         load: 2.00  2.00  2.00
  tp-1a201-02.enst.fr         load: 0.00  0.02  0.00
  ...
=======================================================
  Nodes responded: 39/39
  Avg load  1 min : 0.0674
  Avg load  5 min : 0.0700
  Avg load 15 min : 0.0623
=======================================================
```

Pour que les deux autres membres puissent interroger sans déployer, il leur
suffit d'avoir une copie de `client.py` et de `machines_alive.txt`.

### Arrêter et nettoyer
```bash
./kill.sh
```

- Tue les serveurs en utilisant le PID enregistré dans `~/.server.pid`
  (pas de `pkill -f`, qui risquerait de tuer des processus d'autres étudiants
  ou le shell SSH distant).
- Supprime `~/server.py` du home NFS (donc de toutes les machines).
- Supprime `machines_alive.txt` localement.

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

- **Port élevé** (`54321`) : pas de conflit avec services système (< 1024) ni
  ports classiques.
- **`SO_REUSEADDR`** : permet de relancer le serveur rapidement après kill.
- **IPv6 dual-stack** : les machines ENST ont des adresses IPv6, le serveur
  bind `::` et le socket accepte aussi IPv4.
- **Fichier PID** (`~/.server.pid`) : `kill.sh` utilise `xargs kill` au lieu de
  `pkill -f`, qui serait dangereux (risque de tuer le shell distant ou
  des processus homonymes d'autres utilisateurs).
- **Déploiement séquentiel depuis le jump host** : évite de surcharger
  `ssh.enst.fr` avec 40 connexions parallèles (qui provoquaient
  `fork: Ressource temporairement non disponible`). Chaque saut
  jump → TP est direct et rapide, donc séquentiel reste acceptable.
- **Liste dynamique** : `machines_alive.txt` est reconstruit à chaque
  `deploy.sh` — pas de liste obsolète.
- **`BatchMode=yes` + `ConnectTimeout`** : les scripts échouent vite sur une
  machine injoignable au lieu de bloquer.
- **Côté client, `ThreadPoolExecutor(max_workers=50)`** : interroge les 40
  serveurs en ~5 s, même si certains sont lents.
- **Cleanup robuste** : `trap` sur `EXIT`, `atexit` + handler `SIGTERM` côté
  serveur pour supprimer le fichier PID.

## Respect de l'infrastructure partagée

Conformément aux consignes Télécom Paris (heures non ouvrées pour calculs
lourds) :

- Le serveur est **minuscule** (~40 lignes Python, quasi idle). Il ne consomme
  rien : juste un `accept()` bloquant.
- **Pas d'orchestrateur central** ni de nœud maître — simple pattern
  client/serveur sans état.
- **Cleanup systématique** via `./kill.sh` : ne laisse aucun processus
  orphelin ni fichier sur les machines.
- **Limite raisonnable** (40 machines d'une seule salle) ; pas de scan agressif
  du parc.

## Limitations connues

- Fixé à la salle `1a201` (40 machines). Étendre à 100 machines = ajouter
  d'autres salles dans `machines.txt`.
- Serveur mono-thread : `accept()` séquentiel suffit pour le protocole
  (3 membres × 40 connexions courtes = rien du tout).
- Pas de chiffrement applicatif : la charge CPU n'est pas sensible, et le
  trafic transite déjà sur le réseau interne ENST.
