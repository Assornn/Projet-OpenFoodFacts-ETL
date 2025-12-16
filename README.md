# 🍎 Projet ETL OpenFoodFacts

[![Python](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/)
[![Spark](https://img.shields.io/badge/spark-3.5.0-orange.svg)](https://spark.apache.org/)
[![MySQL](https://img.shields.io/badge/mysql-8.0-blue.svg)](https://www.mysql.com/)
[![Streamlit](https://img.shields.io/badge/streamlit-1.40+-red.svg)](https://streamlit.io/)

> **Projet d'Intégration de Données Massives (TRDE703)**
> Construction d'un pipeline Big Data complet pour l'analyse nutritionnelle de 4 millions de produits.

---

## 🎯 Objectif

Transformer les données brutes d'OpenFoodFacts (**4,1M+ produits**, format CSV complexe) en un Datamart analytique propre et exploitable via un Dashboard interactif.

**Performance atteinte :** Traitement complet de 4,1 Millions de produits en **~35 minutes** sur machine locale (Windows).

---

## 🏗️ Architecture Technique

```mermaid
graph LR
    A[OpenFoodFacts .csv.gz] -->|Spark Bronze| B(Extraction & Typage)
    B -->|Spark Silver| C(Nettoyage & Enrichissement)
    D[Ref. Pays] -.->|Broadcast Join| C
    C -->|Spark Gold| E(Chargement MySQL)
    E -->|SQL| F[(DataMart MySQL)]
    F -->|Connector| G[Dashboard Streamlit]
```
🛠️ Stack Technologique
ETL Engine : Apache Spark 3.5 (PySpark)

Storage : MySQL 8.0 (InnoDB)

Viz : Streamlit + Plotly

Langage : Python 3.10

OS : Compatible Windows / Linux / Mac

✨ Fonctionnalités Clés
1. Extraction & Nettoyage (Bronze/Silver)
Lecture Optimisée : Chargement natif du format compressé .gz.

Normalisation : Trim, Lower, Cast sécurisé (try_cast) pour gérer les données sales sans planter.

Dédoublonnage Intelligent : Conservation de la version la plus récente via Window Function sur last_modified_t.

Enrichissement (Bonus) : Normalisation des pays via Broadcast Join (jointure optimisée en RAM).

Multilingue (Bonus) : Résolution des noms de produits (coalesce FR > EN > Nom par défaut).

2. Contrôle Qualité Avancé
Score de Complétude : Calcul d'un score (0 à 1) basé sur la présence des informations clés (Marque, Nutriments, Nom).

Règles Métier : Détection des incohérences (ex: Sucre > 100g, Energie < 0).

Détection Statistique (Bonus) : Identification des valeurs aberrantes (outliers) via la méthode IQR (Interquartile Range) sur les nutriments.

3. Chargement Optimisé (Gold)
Compatibilité MySQL : Conversion automatique des types complexes (Array → String).

Optimisation JDBC :

Utilisation de .repartition(8) avant écriture pour éviter la saturation mémoire (Java Heap Space) et limiter le nombre de connexions ouvertes.

Paramètres JDBC : batchsize=2000 et rewriteBatchedStatements=true pour la performance d'insertion.

📂 Structure du Projet
Projet-OpenFoodFacts-ETL/
│
├── etl/                    # Code Source PySpark
│   ├── main.py             # Orchestrateur
│   ├── bronze.py           # Extraction
│   ├── silver.py           # Transformation & Qualité
│   └── gold.py             # Chargement MySQL
│
├── dashboard.py            # Dashboard de visualisation Streamlit
├── config.local.yaml       # Configuration (non versionné)
├── etl.zip                 # Package zippé pour Spark Submit
└── README.md               # Documentation

🚀 Installation & Lancement
Prérequis
Java (JDK 8, 11 ou 17)

Python 3.8+

Serveur MySQL local

1. Configuration
Créez un fichier config.local.yaml à la racine :
spark:
  master: "local[*]"
  jars: "file:///C:/chemin/vers/mysql-connector-j-9.x.jar"

mysql:
  host: "localhost"
  port: 3306
  database: "openfoodfacts"
  user: "root"
  password: "votre_mot_de_passe"
  jdbc_url: "jdbc:mysql://localhost:3306/openfoodfacts?rewriteBatchedStatements=true"
  driver: "com.mysql.cj.jdbc.Driver"

openfoodfacts:
  raw_data_path: "data/raw/en.openfoodfacts.org.products.csv.gz"

bronze:
  # limit_rows: 1000  <-- Decommenter pour tester rapidement

2. Lancement du Pipeline ETL

# 1. Création de l'archive de code (pour les workers Spark)
Remove-Item .\etl.zip -Force
Compress-Archive -Path etl -DestinationPath etl.zip -Force

# 2. Exécution du Job Spark
# Adaptez les chemins vers python et le driver MySQL
spark-submit `
   --driver-memory 4g `
   --executor-memory 4g `
   --py-files etl.zip `
   etl/main.py `
   --config config.local.yaml

 Lancement du Dashboard (Bonus)

 python -m streamlit run dashboard.py

 📊 Métriques du Dernier RunLes métriques sont générées automatiquement dans metrics_last_run.json.Voici les résultats sur le dataset complet (Run du 16/12/2025) :MétriqueValeurProduits Lus (Input)4 170 401Doublons Supprimés2 592Produits Finaux (MySQL)4 167 809Score Complétude Moyen73.28 %Anomalies Détectées15 103

 🧠 Stratégie d'Historisation (SCD2)
Note : Pour ce projet, un chargement de type "Snapshot" (Overwrite) a été implémenté pour optimiser la performance du chargement initial (Bulk Load).

Pour une mise en production avec historisation Slowly Changing Dimension Type 2, la stratégie suivante serait appliquée :

Staging : Chargement des nouvelles données dans une table temporaire.

Comparaison : Jointure entre le Staging et le DataMart sur le code produit.

Détection de changement : Comparaison du product_hash (calculé en phase Silver via SHA-256).

Mise à jour :

Si le hash est différent :

UPDATE de l'ancienne ligne : set is_current=0, end_date=NOW().

INSERT de la nouvelle ligne : set is_current=1, start_date=NOW(), end_date=NULL.


COMMANDE A LANCER :

Remove-Item .\etl.zip -Force                                                                                                                                   
Compress-Archive -Path etl -DestinationPath etl.zip -Force
C:\spark\bin\spark-submit.cmd `     
>>    --master local[*] `
>>    --conf spark.pyspark.python=C:\Users\abraure\AppData\Local\Programs\Python\Python310\python.exe `
>>    --conf spark.pyspark.driver.python=C:\Users\abraure\AppData\Local\Programs\Python\Python310\python.exe `
>>    --jars C:\spark\jars\mysql-connector-j-9.5.0.jar `
>>    --driver-class-path C:\spark\jars\mysql-connector-j-9.5.0.jar `
>>    --conf spark.executor.extraClassPath=C:\spark\jars\mysql-connector-j-9.5.0.jar `
>>    --conf spark.driver.extraClassPath=C:\spark\jars\mysql-connector-j-9.5.0.jar `
>>    --driver-memory 6g `
>>    --executor-memory 6g `
>>    --py-files etl.zip `
>>    etl/main.py `
>>    --config config.local.yaml
