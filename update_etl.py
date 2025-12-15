#!/usr/bin/env python3
"""
Script de mise à jour automatique de l'ETL OpenFoodFacts
- Crée les modules Bronze/Silver/Gold séparés
- Met à jour la configuration pour utiliser le CSV officiel
- Télécharge automatiquement les données
"""

import os
import sys
from pathlib import Path

def create_file(filepath, content):
    """Crée un fichier avec son contenu"""
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"  ✓ {filepath}")

def main():
    print("="*70)
    print("🔄 MISE À JOUR AUTOMATIQUE DE L'ETL OPENFOODFACTS")
    print("="*70)
    print()
    
    # 1. Créer bronze.py
    print("📦 Création de etl/bronze.py...")
    bronze_content = '''"""
Phase BRONZE - Extraction des données OpenFoodFacts CSV
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)


class BronzeExtractor:
    """Extracteur Bronze - Lecture CSV OpenFoodFacts"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.metrics = {}
    
    def extract_csv(self, source_path: str) -> DataFrame:
        """
        Extrait depuis le CSV OpenFoodFacts
        Format: TSV avec headers
        """
        logger.info(f"=== PHASE BRONZE: Extraction CSV ===")
        logger.info(f"Source: {source_path}")
        
        try:
            # Lecture CSV OpenFoodFacts (séparateur TAB)
            df = self.spark.read.csv(
                source_path,
                header=True,
                sep='\\t',
                quote='"',
                escape='\\\\',
                inferSchema=False,
                multiLine=True
            )
            
            count = df.count()
            self.metrics['products_read'] = count
            self.metrics['source_path'] = source_path
            
            logger.info(f"✓ {count:,} produits extraits")
            
            # Afficher les colonnes disponibles
            logger.info(f"Colonnes disponibles: {len(df.columns)}")
            
            df.cache()
            return df
            
        except Exception as e:
            logger.error(f"✗ Erreur extraction: {e}")
            raise
    
    def extract(self, source_path: str = None) -> DataFrame:
        """Point d'entrée principal"""
        if source_path is None:
            source_path = self.config['openfoodfacts']['raw_data_path']
        
        return self.extract_csv(source_path)
    
    def get_metrics(self) -> dict:
        return self.metrics
'''
    create_file("etl/bronze.py", bronze_content)
    
    # 2. Créer silver.py
    print("📦 Création de etl/silver.py...")
    silver_content = '''"""
Phase SILVER - Transformation et nettoyage
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)


class SilverTransformer:
    """Transformateur Silver"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.metrics = {}
    
    def transform(self, df_bronze: DataFrame) -> DataFrame:
        """Pipeline de transformation complet"""
        logger.info("=== PHASE SILVER: Transformation ===")
        
        count_initial = df_bronze.count()
        
        # Filtrage des codes valides
        df = df_bronze.filter(
            col("code").isNotNull() & 
            (length(col("code")) >= 8)
        )
        
        # Extraction nom produit (priorité français)
        df = df.withColumn("product_name_clean",
            coalesce(
                col("product_name_fr"),
                col("product_name_en"),
                col("product_name")
            )
        )
        
        # Extraction nutriments principaux
        for nutrient in ['energy-kcal_100g', 'fat_100g', 'sugars_100g', 
                        'salt_100g', 'proteins_100g', 'carbohydrates_100g']:
            df = df.withColumn(
                nutrient.replace('-', '_').replace('energy_kcal', 'energy_kcal'),
                col(nutrient).cast('double')
            )
        
        # Dédoublonnage par code
        window = Window.partitionBy("code").orderBy(desc("last_modified_t"))
        df = df.withColumn("row_num", row_number().over(window))
        df = df.filter(col("row_num") == 1).drop("row_num")
        
        # Score de complétude
        df = df.withColumn("completeness_score",
            (
                when(col("product_name_clean").isNotNull(), 1).otherwise(0) +
                when(col("brands").isNotNull(), 1).otherwise(0) +
                when(col("energy_kcal_100g").isNotNull(), 1).otherwise(0) +
                when(col("fat_100g").isNotNull(), 1).otherwise(0) +
                when(col("sugars_100g").isNotNull(), 1).otherwise(0)
            ) / 5.0
        )
        
        count_final = df.count()
        self.metrics['products_input'] = count_initial
        self.metrics['products_filtered'] = count_final
        self.metrics['avg_completeness'] = df.agg(avg("completeness_score")).first()[0]
        
        logger.info(f"✓ {count_final:,} produits après transformation")
        logger.info(f"✓ Complétude moyenne: {self.metrics['avg_completeness']:.2%}")
        
        df.cache()
        return df
    
    def get_metrics(self) -> dict:
        return self.metrics
'''
    create_file("etl/silver.py", silver_content)
    
    # 3. Créer gold.py
    print("📦 Création de etl/gold.py...")
    gold_content = '''"""
Phase GOLD - Chargement Datamart MySQL
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
import logging

logger = logging.getLogger(__name__)


class GoldLoader:
    """Chargeur Gold - MySQL"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.metrics = {}
        
        self.jdbc_url = config['mysql']['jdbc_url']
        self.jdbc_props = {
            "user": config['mysql']['user'],
            "password": config['mysql']['password'],
            "driver": "com.mysql.cj.jdbc.Driver"
        }
    
    def load_dim_brand(self, df: DataFrame):
        """Charge dimension marques"""
        logger.info("Chargement dim_brand...")
        
        df_brand = df.select(explode(split(col("brands"), ",")).alias("brand_name")).distinct()
        df_brand = df_brand.filter(col("brand_name").isNotNull())
        df_brand = df_brand.withColumn("brand_name", trim(col("brand_name")))
        df_brand = df_brand.withColumn("brand_sk", monotonically_increasing_id())
        
        count = df_brand.count()
        df_brand.write.jdbc(self.jdbc_url, "dim_brand", mode="overwrite", properties=self.jdbc_props)
        
        logger.info(f"✓ {count:,} marques chargées")
        self.metrics['dim_brand_loaded'] = count
        return df_brand
    
    def load_fact_nutrition(self, df: DataFrame):
        """Charge table de faits"""
        logger.info("Chargement fact_nutrition_snapshot...")
        
        df_fact = df.select(
            monotonically_increasing_id().alias("fact_id"),
            col("code").alias("product_code"),
            col("energy_kcal_100g"),
            col("fat_100g"),
            col("sugars_100g"),
            col("salt_100g"),
            col("proteins_100g"),
            col("nutriscore_grade"),
            col("completeness_score")
        )
        
        count = df_fact.count()
        df_fact.write.jdbc(self.jdbc_url, "fact_nutrition_snapshot", mode="append", properties=self.jdbc_props)
        
        logger.info(f"✓ {count:,} faits chargés")
        self.metrics['fact_nutrition_loaded'] = count
    
    def load(self, df_silver: DataFrame):
        """Charge tout le datamart"""
        logger.info("=== PHASE GOLD: Chargement ===")
        
        self.load_dim_brand(df_silver)
        self.load_fact_nutrition(df_silver)
        
        logger.info("✓ Datamart chargé avec succès")
    
    def get_metrics(self) -> dict:
        return self.metrics
'''
    create_file("etl/gold.py", gold_content)
    
    # 4. Créer main.py (orchestrateur)
    print("📦 Création de etl/main.py (nouveau)...")
    main_content = '''"""
ETL OpenFoodFacts - Orchestrateur Principal
"""

from pyspark.sql import SparkSession
import yaml
import json
import logging
from datetime import datetime
from pathlib import Path

from bronze import BronzeExtractor
from silver import SilverTransformer
from gold import GoldLoader


class OpenFoodFactsETL:
    """Orchestrateur ETL"""
    
    def __init__(self, config_path='config.yaml'):
        self._setup_logging()
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        self.metrics = {
            'run_timestamp': datetime.now().isoformat(),
            'status': 'started'
        }
    
    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/etl.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _load_config(self, config_path):
        # Priorité config.local.yaml
        local = Path('config.local.yaml')
        if local.exists():
            config_path = 'config.local.yaml'
        
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _create_spark_session(self):
        builder = SparkSession.builder \\
            .appName(self.config['spark']['app_name']) \\
            .master(self.config['spark']['master']) \\
            .config("spark.driver.memory", self.config['spark']['driver_memory'])
        
        mysql_jar = self.config['spark'].get('mysql_jar')
        if mysql_jar and Path(mysql_jar).exists():
            builder = builder.config("spark.jars", mysql_jar)
        
        return builder.getOrCreate()
    
    def run(self):
        """Exécute le pipeline complet"""
        self.logger.info("\\n" + "="*60)
        self.logger.info("🚀 DÉMARRAGE ETL OPENFOODFACTS")
        self.logger.info("="*60)
        
        start = datetime.now()
        
        try:
            # Bronze
            extractor = BronzeExtractor(self.spark, self.config)
            df_bronze = extractor.extract()
            self.metrics['bronze'] = extractor.get_metrics()
            
            # Silver
            transformer = SilverTransformer(self.spark, self.config)
            df_silver = transformer.transform(df_bronze)
            self.metrics['silver'] = transformer.get_metrics()
            
            # Gold
            loader = GoldLoader(self.spark, self.config)
            loader.load(df_silver)
            self.metrics['gold'] = loader.get_metrics()
            
            self.metrics['status'] = 'success'
            
        except Exception as e:
            self.logger.error(f"✗ Erreur: {e}")
            self.metrics['status'] = 'failed'
            self.metrics['error'] = str(e)
            raise
        
        finally:
            duration = (datetime.now() - start).total_seconds()
            self.metrics['duration_seconds'] = duration
            
            # Sauvegarder métriques
            output_dir = Path('output/metrics')
            output_dir.mkdir(parents=True, exist_ok=True)
            
            metrics_file = output_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(metrics_file, 'w') as f:
                json.dump(self.metrics, f, indent=2)
            
            self.logger.info(f"\\n✓ Durée: {duration:.2f}s")
            self.logger.info(f"✓ Statut: {self.metrics['status']}")
            
            self.spark.stop()


if __name__ == "__main__":
    etl = OpenFoodFactsETL()
    etl.run()
'''
    create_file("etl/main.py", main_content)
    
    # 5. Mettre à jour config.yaml
    print("📦 Mise à jour de config.yaml...")
    config_content = '''# Configuration OpenFoodFacts ETL - CSV Officiel

spark:
  app_name: "OpenFoodFacts ETL"
  master: "local[*]"
  driver_memory: "4g"
  executor_memory: "4g"
  mysql_jar: "/path/to/mysql-connector-java-8.0.33.jar"

mysql:
  host: "localhost"
  port: 3306
  database: "openfoodfacts"
  user: "root"
  password: "password"
  jdbc_url: "jdbc:mysql://localhost:3306/openfoodfacts"

openfoodfacts:
  # URL officielle du CSV OpenFoodFacts
  source_url: "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
  
  # Chemin local après téléchargement
  raw_data_path: "data/raw/en.openfoodfacts.org.products.csv"
  
  format: "csv"

quality:
  completeness:
    product_name: 0.95
    nutrients_key: 0.70
  
  rules:
    bounds:
      sugars_100g: [0, 100]
      salt_100g: [0, 25]
      fat_100g: [0, 100]
      proteins_100g: [0, 100]
      energy_kcal_100g: [0, 900]

pipeline:
  phases:
    - bronze
    - silver
    - gold
  
  execution:
    mode: "batch"
    checkpoint_enabled: true
'''
    create_file("config.yaml", config_content)
    
    # 6. Créer script de téléchargement
    print("📦 Création de scripts/download_data.sh...")
    download_script = '''#!/bin/bash
# Script de téléchargement des données OpenFoodFacts CSV

echo "📥 Téléchargement du CSV OpenFoodFacts..."

# Créer le dossier
mkdir -p data/raw

# Télécharger (environ 1.5 GB compressé)
echo "Téléchargement depuis: https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
curl -L -o data/raw/en.openfoodfacts.org.products.csv.gz \\
    https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz

# Décompresser
echo "Décompression..."
gunzip -f data/raw/en.openfoodfacts.org.products.csv.gz

echo "✓ Téléchargement terminé!"
echo "Fichier: data/raw/en.openfoodfacts.org.products.csv"

# Afficher les premières lignes
echo ""
echo "Aperçu du fichier:"
head -n 3 data/raw/en.openfoodfacts.org.products.csv
'''
    create_file("scripts/download_data.sh", download_script)
    
    # Version PowerShell
    print("📦 Création de scripts/download_data.ps1...")
    download_ps1 = '''# Script PowerShell de téléchargement OpenFoodFacts CSV

Write-Host "📥 Téléchargement du CSV OpenFoodFacts..." -ForegroundColor Green

# Créer le dossier
New-Item -ItemType Directory -Force -Path data/raw | Out-Null

# URL source
$url = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
$gzFile = "data/raw/en.openfoodfacts.org.products.csv.gz"
$csvFile = "data/raw/en.openfoodfacts.org.products.csv"

Write-Host "Téléchargement depuis: $url"
Write-Host "⚠️  Fichier volumineux (~1.5 GB), cela peut prendre du temps..."

# Télécharger
Invoke-WebRequest -Uri $url -OutFile $gzFile -UseBasicParsing

Write-Host "✓ Téléchargement terminé"
Write-Host "Décompression..."

# Décompresser avec .NET
Add-Type -AssemblyName System.IO.Compression.FileSystem
$gzStream = New-Object System.IO.FileStream($gzFile, [System.IO.FileMode]::Open)
$output = New-Object System.IO.FileStream($csvFile, [System.IO.FileMode]::Create)
$gzipStream = New-Object System.IO.Compression.GZipStream($gzStream, [System.IO.Compression.CompressionMode]::Decompress)

$buffer = New-Object byte[](1024)
while ($true) {
    $read = $gzipStream.Read($buffer, 0, 1024)
    if ($read -le 0) { break }
    $output.Write($buffer, 0, $read)
}

$gzipStream.Close()
$output.Close()
$gzStream.Close()

Write-Host "✓ Décompression terminée" -ForegroundColor Green
Write-Host "Fichier: $csvFile"

# Supprimer le .gz
Remove-Item $gzFile

Write-Host "✓ Terminé!" -ForegroundColor Green
'''
    create_file("scripts/download_data.ps1", download_ps1)
    
    # 7. Mettre à jour README
    print("📦 Mise à jour de README.md...")
    readme_update = '''

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
.\\scripts\\download_data.ps1
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
'''
    
    # Ajouter au README existant
    with open('README.md', 'a', encoding='utf-8') as f:
        f.write(readme_update)
    print("  ✓ README.md mis à jour")
    
    # 8. Créer QUICKSTART simplifié
    print("📦 Mise à jour de QUICKSTART.md...")
    quickstart = '''# 🚀 Guide de Démarrage Rapide - OpenFoodFacts ETL

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
.\\scripts\\download_data.ps1

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
'''
    create_file("QUICKSTART.md", quickstart)
    
    print()
    print("="*70)
    print("✅ MISE À JOUR TERMINÉE AVEC SUCCÈS!")
    print("="*70)
    print()
    print("📋 Fichiers créés/modifiés:")
    print("  ✓ etl/bronze.py (nouveau)")
    print("  ✓ etl/silver.py (nouveau)")
    print("  ✓ etl/gold.py (nouveau)")
    print("  ✓ etl/main.py (remplacé)")
    print("  ✓ config.yaml (mis à jour pour CSV)")
    print("  ✓ scripts/download_data.sh (nouveau)")
    print("  ✓ scripts/download_data.ps1 (nouveau)")
    print("  ✓ README.md (mis à jour)")
    print("  ✓ QUICKSTART.md (mis à jour)")
    print()
    print("🎯 Prochaines étapes:")
    print("  1. Télécharger les données: .\\scripts\\download_data.ps1")
    print("  2. Configurer config.local.yaml")
    print("  3. Lancer: python etl/main.py")
    print()
    print("📤 Pour Git:")
    print("  git add .")
    print('  git commit -m "refactor: Modularize ETL and adapt for CSV source"')
    print("  git push origin main")
    print()
    print("✨ Bon courage!")


if __name__ == "__main__":
    main()