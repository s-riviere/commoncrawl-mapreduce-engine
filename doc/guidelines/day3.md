# Formalisation et Implémentation MapReduce

Date : 29/05/2026

## 1. Formalisation et Collaboration
* **Modélisation du protocole :** Utiliser le site [sequencediagram.org](https://www.sequencediagram.org) pour formaliser précisément le protocole.
* **Gestion d'équipe :** Définir une méthode de répartition des tâches entre les membres du groupe et mettre en place un système de partage des découvertes et de l'état d'avancement du développement.

---

## 2. Points Techniques Délicats (Tricky Parts)
* **Bootstrap du système :** Déterminer comment initialiser le système (cela nécessitera une version améliorée du protocole).
* **Lecture à distance pour la phase Reduce :** Trouver une solution pour la lecture distante lors de la phase de réduction. 
    > **Rappel :** Les tâches de *Map* écrivent sur les disques locaux (se référer à la Figure 1 de la publication *MapReduce* de Jeffrey Dean et Sanjay Ghemawat).

---

## 3. Implémentation et Données
* **Données sources :** Faire fonctionner le système sur des segments (*splits*) issus de **Common Crawl**.
    > **Rappel :** Certains segments sont stockés sur un serveur local et sont accessibles en montant le dossier `/cal/commoncrawl`.
* **Algorithmes :** 1.  Implémenter d'abord un algorithme de **fréquence des mots** (*word frequency*).  
    2.  Implémenter d'autres algorithmes par la suite.

---

## 4. Objectifs à Long Terme (Avancé)
Pour ceux qui progressent rapidement vers les étapes suivantes des travaux pratiques :

* **Tolérance aux pannes :** Concevoir, implémenter et tester un mécanisme de tolérance aux pannes (impliquant un nouveau design du protocole).
* **Comparaison avec Apache Kafka Streams :**
    * Déployer des nœuds **Apache Kafka Streams**.
    * Exécuter le même algorithme de fréquence des mots sur les mêmes segments de données.
    * Comparer les résultats, la vitesse d'exécution et la difficulté de mise en œuvre entre les deux systèmes.
    * *Point délicat :* Déployer les nœuds Kafka Streams sans posséder les privilèges *root* sur les machines de l'école.
