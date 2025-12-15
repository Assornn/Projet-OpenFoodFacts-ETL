# 🚀 Guide de Démarrage Rapide

## Installation (15 minutes)

### 1. Prérequis
```bash
python --version  # >= 3.8
mysql --version   # >= 8.0
```

### 2. Installation
```bash
pip install -r requirements.txt
wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/mysql-connector-java-8.0.33.jar
```

### 3. MySQL Setup
```bash
mysql -u root -p < sql/ddl/create_datamart.sql
```

### 4. Télécharger Sample
```bash
mkdir -p data/raw
# Télécharger et extraire 10k premières lignes pour test
```

### 5. Lancer ETL
```bash
python etl/main.py
```

## Vérification
```bash
mysql -u root -p openfoodfacts
SELECT COUNT(*) FROM fact_nutrition_snapshot;
```

Voir README.md pour la documentation complète.
