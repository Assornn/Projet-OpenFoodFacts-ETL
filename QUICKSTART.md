# 🚀 Guide de Démarrage Rapide - OpenFoodFacts ETL

## ⚡ Installation Express (15 minutes)

### 1. Prérequis
- Python 3.8+
- MySQL 8.0+
- 10 GB d'espace disque libre

### 2. Installation Python
```bash
pip install -r requirements.txt
```

### 3. Télécharger données OpenFoodFacts
```bash
# Windows (PowerShell)
.\scripts\download_data.ps1

# Linux/Mac
chmod +x scripts/download_data.sh
./scripts/download_data.sh
```
⏱️ Temps: ~15-30 min selon votre connexion

### 4. Configuration MySQL
```bash
mysql -u root -p < sql/ddl/create_datamart.sql
```

### 5. Configuration
Modifier `config.local.yaml` avec vos vrais paramètres:
```yaml
mysql:
  password: "votre_mot_de_passe"

spark:
  mysql_jar: "C:/chemin/vers/mysql-connector-j-8.0.33.jar"
```

### 6. Lancer l'ETL
```bash
python etl/main.py
```

⏱️ Durée: ~30-60 minutes (2,8M produits)

## 📊 Vérifier les résultats

```sql
mysql -u root -p openfoodfacts

SELECT COUNT(*) FROM fact_nutrition_snapshot;
SELECT COUNT(*) FROM dim_brand;
```

## 🎯 Test Rapide (Sample)

Pour tester sans tout télécharger:
```bash
# Prendre seulement 10k lignes
head -n 10001 data/raw/en.openfoodfacts.org.products.csv > data/raw/sample.csv

# Modifier config.local.yaml:
# raw_data_path: "data/raw/sample.csv"

python etl/main.py
```
⏱️ Temps: ~2-5 minutes
