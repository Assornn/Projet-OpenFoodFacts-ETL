# Projet ETL OpenFoodFacts

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Spark](https://img.shields.io/badge/spark-3.5.0-orange.svg)](https://spark.apache.org/)
[![MySQL](https://img.shields.io/badge/mysql-8.0+-blue.svg)](https://www.mysql.com/)

> Projet d'intégration de données massives (Big Data) - Module TRDE703  
> Construction d'un datamart analytique avec Apache Spark et MySQL

## 🎯 Objectif

Construire un datamart "OpenFoodFacts Nutrition & Qualité" permettant l'analyse des données nutritionnelles de 2,8M+ produits alimentaires.

## 🏗️ Architecture

- **Source:** OpenFoodFacts (JSON/CSV)
- **ETL:** Apache Spark (PySpark)
- **Datawarehouse:** MySQL 8.0 (Schéma en étoile)
- **Qualité:** 11+ règles de validation automatisées

## 🚀 Installation Rapide

```bash
# 1. Cloner et installer
git clone https://github.com/Assornn/Projet-OpenFoodFacts-ETL.git
cd Projet-OpenFoodFacts-ETL
pip install -r requirements.txt

# 2. Configuration MySQL
mysql -u root -p < sql/ddl/create_datamart.sql

# 3. Télécharger données (sample)
mkdir -p data/raw
# Voir QUICKSTART.md pour les détails

# 4. Lancer l'ETL
python etl/main.py
```

## 📊 Fonctionnalités

✅ Pipeline ETL 3 couches (Bronze/Silver/Gold)  
✅ Schéma en étoile optimisé  
✅ SCD Type 2 pour historisation  
✅ Contrôle qualité automatisé  
✅ 10 requêtes analytiques prêtes  
✅ Tests unitaires (pytest)  
✅ Documentation complète  

## 📁 Structure du Projet

```
Projet-OpenFoodFacts-ETL/
├── etl/              # Code ETL Spark
├── sql/              # Scripts SQL
├── docs/             # Documentation
├── tests/            # Tests unitaires
├── data/             # Données (gitignored)
└── output/           # Résultats et rapports
```

## 📚 Documentation

- [Guide de démarrage rapide](QUICKSTART.md)
- [Architecture détaillée](docs/architecture.md)
- [Dictionnaire de données](docs/data-dictionary.md)

## 🧪 Tests

```bash
pytest tests/ -v
```

## 👥 Équipe

- **Développement ETL**
- **Modélisation Données**
- **Qualité & Tests**

## 📝 Licence

Projet pédagogique - M1 EISI/CDPIA/CYBER 2025-2026


## 📥 Téléchargement des Données

Le projet utilise le **CSV officiel OpenFoodFacts** :
- URL: https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz
- Taille: ~1.5 GB compressé, ~8 GB décompressé
- Contenu: ~2,8M produits du monde entier

### Téléchargement automatique

**Linux/Mac:**
```bash
chmod +x scripts/download_data.sh
./scripts/download_data.sh
```

**Windows (PowerShell):**
```powershell
.\scripts\download_data.ps1
```

### Téléchargement manuel

1. Télécharger: https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz
2. Placer dans `data/raw/`
3. Décompresser: `gunzip en.openfoodfacts.org.products.csv.gz`

## 🚀 Exécution

```bash
# 1. Télécharger les données
./scripts/download_data.sh

# 2. Lancer l'ETL
python etl/main.py
```

## 📊 Format des Données

Le CSV OpenFoodFacts contient 180+ colonnes avec:
- Codes-barres (code)
- Noms produits (product_name, product_name_fr, product_name_en)
- Marques (brands)
- Catégories (categories)
- Nutriments (*_100g)
- Scores (nutriscore_grade, nova_group, ecoscore_grade)
