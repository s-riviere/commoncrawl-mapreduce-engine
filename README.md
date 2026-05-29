# MapReduce — Challenge 1 : charge CPU distribuée

Déploiement d'un serveur de charge CPU sur les machines des salles TP de
Télécom Paris, collecte de la charge (`loadavg`) de chaque nœud depuis un
client, et calcul de la charge moyenne du cluster.

## Objectif du TP

- Fichier contenant les noms des machines `tp-*.enst.fr`.
- Script de déploiement : **1 `ssh -J` par machine** pour copier le serveur sur
  le disque local, puis **1 `ssh` par machine** pour le démarrer.
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
  deploy.sh  ──ssh -J ssh.enst.fr──────────────► machines TP depuis votre poste,
                                                  copie server.py vers /tmp/<login>/slr207-project
             ──ssh -J ssh.enst.fr──────────────► machines TP depuis votre poste,
                                                  lance nohup server depuis le disque local
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
  kill.sh    ──ssh -J ssh.enst.fr►  machines TP depuis votre poste,
                                     tue chaque serveur par PID
                                     (fichier /tmp/<login>/slr207-project/.server.pid)
             nettoie localement    : supprime machines_alive.txt
```

Chaque machine reçoit sa propre copie de `server.py` dans
`/tmp/<login>/slr207-project`, ce qui évite toute dépendance à un home NFS.

## Fichiers

| Fichier | Rôle |
|---------|------|
| `machines.txt` | Univers des machines (40 × `tp-1a201-XX.enst.fr`) |
| `server.py` | Écoute TCP sur le port choisi, envoie `load1 load5 load15` |
| `client.py` | Interroge en parallèle toutes les machines vivantes, affiche les stats |
| `deploy.sh` | Copie `server.py` sur le disque local de chaque machine via `ssh -J`, puis le lance via `ssh -J` ; génère `machines_alive.txt` |
| `kill.sh` | Tue les serveurs via leur PID avec SSH local via `-J`, nettoie `/tmp/<login>/slr207-project` et les fichiers locaux |

## Pré-requis

Remplacer `<login>` ci-dessous par votre login Télécom Paris (ex. `prenom-25`).

1. **Compte Télécom Paris** actif.
2. **Clé SSH locale** copiée sur `ssh.enst.fr` :
   ```bash
   ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
   ssh-copy-id -i ~/.ssh/id_ed25519.pub <login>@ssh.enst.fr
   ```
3. **Config SSH locale** (`~/.ssh/config`) :
   ```
   Host telecom
       HostName ssh.enst.fr
       User <login>

   Host tp-*
       ProxyJump telecom
       User <login>
   ```
4. **Mettre à jour les scripts** : remplacer la valeur de `REMOTE_USER` en
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
[1/2] Copying server.py to each machine's local disk...
[2/2] Starting server on each machine from this computer...
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

- Tue les serveurs en utilisant le PID enregistré dans
  `/tmp/<login>/slr207-project/.server.pid`
  (pas de `pkill -f`, qui risquerait de tuer des processus d'autres étudiants
  ou le shell SSH distant).
- Supprime `server.py` et `.server.pid` de `/tmp/<login>/slr207-project` sur
  chaque machine.
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
- **Fichier PID** (`/tmp/<login>/slr207-project/.server.pid`) : `kill.sh`
  utilise `fuser -k` sur le port puis supprime le fichier PID, ce qui évite
  de dépendre du home NFS.
- **Déploiement séquentiel depuis votre poste via `ProxyJump`** : toutes les
  connexions SSH sont initiées localement, et `ssh.enst.fr` ne fait que relayer
  le trafic vers les machines TP. La boucle reste séquentielle pour éviter de
  lancer 40 connexions en parallèle.
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
