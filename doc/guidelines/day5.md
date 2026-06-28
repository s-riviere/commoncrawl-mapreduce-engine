# Objectifs des Dernières Sessions, Robustesse et Évaluation Comparative

Date : 12/06/2026

## 1. Robustesse, Tolérance aux Pannes et Passage à l'Échelle
* **Tests aux limites :** Pousser le système développé jusqu'à sa rupture afin de cartographier ses points de défaillance.
* **Analyse des pannes :** Identifier avec précision les causes structurelles ou logicielles de chaque plantage (saturation mémoire, rupture de sockets, timeouts).
* **Mécanisme de résilience :** Implémenter une stratégie de tolérance aux pannes permettant au système de gérer les déconnexions ou la perte de nœuds sans interrompre le traitement global.

---

## 2. Infrastructure Apache Kafka Streams
* **Déploiement natif :** Concevoir des scripts d'automatisation (`deploy` et `clean`) pour une installation locale (sans conteneurisation Docker, indisponible sur les machines locales).
* **Validation initiale :** Valider l'infrastructure via le cas d'usage standard *WordCount* fourni dans la documentation officielle de Kafka.

---

## 3. Cas d'Usage Avancés (Analyse de Données)
Définir et implémenter trois scénarios d'analyse distincts du simple décompte de mots sur le jeu de données Common Crawl :
* **Cas d'usage 1 (ex. Identification des langues) :** Détection de la répartition linguistique des pages web.
* **Cas d'usage 2 (ex. Popularité / Classement) :** Évaluation de la fréquence de citation de certains domaines ou mots-clés spécifiques.
* **Cas d'usage 3 (ex. Volumétrie / Densité) :** Analyse de la taille des pages, de la densité du texte ou du ratio balises/contenu.

---

## 4. Analyse Comparative des Paradigmes
Établir une matrice de comparaison technique mesurant les performances, la complexité et les limites de trois approches :
* **Système interne :** L'architecture sur mesure développée lors des sessions précédentes.
* **Apache Hadoop :** Modèle de référence pour le traitement par lots (*Batch Processing*).
* **Kafka Streams :** Modèle de référence pour le traitement de flux en temps réel (*Stream Processing*).

---

## 5. Livrables et Soutenance Finale
* **Rapport écrit final :** Document de synthèse obligatoire comprenant :
    * L'architecture détaillée du système sur mesure.
    * Le design du protocole réseau et de communication.
    * Les métriques de performance collectées.
    * Le graphique de validation empirique de la loi d'Amdahl.
    * Le retour d'expérience sur les difficultés rencontrées (*pain points*) et les causes des pannes.
    * L'explication et l'analyse des résultats des 3 cas d'usage.
    * L'étude comparative détaillée (*Batch vs Stream*).
* **Démonstration et Soutenance :** Présentation orale lors de la dernière session structurée en deux phases strictes :
    * 10 minutes de présentation technique.
    * 10 minutes de questions/réponses.
