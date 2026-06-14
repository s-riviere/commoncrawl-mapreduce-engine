# Métriques de Performance et Validation de la Loi d'Amdahl

Date : 05/06/2026

## 1. Métriques de Performance et Profilage
* **Chronométrage des étapes :** Implémenter une mesure précise du temps d'exécution pour chaque phase du cycle de vie du système :
    * Déploiement (deploy) et nettoyage (cleaning).
    * Lecture et écriture de fichiers.
    * Synchronisation et phases d'attente.
    * Communications réseau et transferts de données.
    * Calculs effectifs (phases de Map et de Reduce).
* **Détection des goulots d'étranglement :** Analyser les mesures obtenues pour identifier les étapes critiques qui limitent les performances globales du système.
* **Optimisation et Débogage :** Corriger et optimiser le système sur la base des goulots d'étranglement détectés lors de la phase de profilage.

---

## 2. Infrastructure et Accès Direct aux Données
* **Obsolescence du stockage interne :** Supprimer définitivement l'utilisation du serveur NFS interne, ce dernier saturant lors des accès concurrents massifs (lectures simultanées de centaines de fichiers).
* **Intégration Common Crawl :** Analyser l'architecture de Common Crawl pour charger les données directement depuis les serveurs d'Amazon Web Services (AWS S3) vers les nœuds de calcul, éliminant ainsi l'intermédiaire NFS.

---

## 3. Scénarios de Test et Validation
Évaluer le comportement et la robustesse du système face à différentes échelles de charge en utilisant les configurations suivantes :
* **Micro-segments (Proof of Concept) :** Validation initiale avec des fichiers de taille minimale contenant seulement quelques mots par segment.
* **Segments réels (Échelle réduite) :** Exécution sur 2 à 3 segments réels issus de Common Crawl.
* **Passage à l'échelle (Stress Test) :** Évaluation des limites du système sur des volumes massifs (100, 1000 segments ou plus).

---

## 4. Analyse Théorique et Empirique : Loi d'Amdahl
* **Compréhension théorique :** Modéliser le gain de vitesse théorique (speedup), comprendre l'impact de la fraction séquentielle non parallélisable et définir la méthodologie de comparaison entre les versions du système.
* **Preuve empirique et Graphiques :**
    * Établir un point de référence initial : Speedup = 1 pour un nombre de nœuds = 1.
    * Tracer la courbe expérimentale représentant le speedup en fonction de la variation du nombre de nœuds de calcul.
    * Pour garantir la validité du graphique et de la loi d'Amdahl, le jeu de données utilisé doit être strictement identique pour l'ensemble des points d'une même courbe. Toute modification de la taille ou de la nature du jeu de données invalide la comparaison.
