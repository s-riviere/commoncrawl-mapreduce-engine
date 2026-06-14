# Infrastructure et Déploiement

Date : 24/04/2026

## 1. Prérequis et Configuration Technique

### Réseau et Accès
* **Connexion :** Utilisez impérativement le Wi-Fi **campus telecom**. Ne pas utiliser *eduroam*, car ce dernier n'est pas sur le même réseau que les machines de l'école.
* **Configuration SSH :**
    * Configurez votre SSH pour **ignorer la vérification de l'empreinte** (*fingerprint*) afin d'éviter de devoir valider par « yes » lors de la première connexion aux machines.
    * Configurez l'authentification par **clés SSH (publique/privée)** pour vous connecter aux machines de l'école sans avoir à saisir votre mot de passe à chaque fois.

### Outils de Développement (VSCode & GitHub Copilot)
* **Licence :** L'accès à GitHub Copilot se fait via l'organisation « Telecom Paris ».
* **Rattrapage (si absent) :** Envoyez votre identifiant GitHub (*login name*) à l'enseignant pour être ajouté à l'organisation. Une fois l'invitation acceptée, cochez la case « ask for a copilot license ».
* **Concepts clés démontrés :**
    * Utilisation de l'extension Copilot dans VSCode sous différents modes (*ask*, *agent*, *plan*).
    * Fonctionnement du mode *agent* (validation requise pour les actions système comme l'exécution de commandes, la modification de fichiers, les recherches internet).
    * Utilisation des différents modèles disponibles et rôle des *thinking tokens* pour les étapes de raisonnement du LLM.

---

## 2. Le « Big Project » : Premier Défi (En Groupe)

L'objectif actuel est de déployer un système de collecte de charge CPU sur 100 machines de l'école.

### Étape 1 : Liste des machines
* Générez un fichier contenant la liste de **100 machines actives** de `tp.telecom-paris.fr`.
* **Méthode :** Analysez les requêtes de votre navigateur pour identifier l'API (format JSON brut) accessible via `https://tp.telecom-paris.fr/ajax.php`. Un script peut être utilisé pour automatiser cela.
* **Format :** Utilisez les noms canoniques complets, par exemple : `tp-1a201-05.enst.fr`.

### Étape 2 : Script de déploiement
* Le répertoire personnel (`home directory`) étant sur un système de fichiers partagé (**NFS**), un fichier copié y est accessible depuis toutes les machines de l'école.
* **Action 1 (SCP) :** Utilisez **une seule commande SCP** pour copier le fichier du serveur dans votre répertoire personnel.
* **Action 2 (SSH) :** Exécutez **100 commandes SSH** (une par machine) pour lancer le serveur sur les 100 machines cibles.
* **Configuration du serveur :** Les serveurs doivent écouter sur un port TCP spécifique (veillez à ce que le port choisi ne soit pas déjà utilisé).

### Étape 3 : Client et Protocole
* **Connexion :** Chaque membre du groupe doit connecter un client aux 100 serveurs via le port spécifié.
* **Protocole réseau :** Dès que la connexion TCP est établie, le serveur doit envoyer sa charge CPU moyenne (*load average*) des dernières 1, 5 et 15 minutes (identique aux données de la commande `uptime`).
* **Rôle du client :** Récupérer ces données pour calculer et afficher les statistiques globales (charge moyenne des 100 nœuds pour 1, 5 et 15 minutes).

### Étape 4 : Script de nettoyage
* Créez un script permettant de **tuer (kill) tous les serveurs déployés**. Ce script est indispensable pour nettoyer les machines avant de redéployer une nouvelle version du serveur en cas de mise à jour.
