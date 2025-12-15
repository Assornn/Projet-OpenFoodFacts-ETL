"""
ETL OpenFoodFacts - Pipeline Principal
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import yaml
from datetime import datetime

class OpenFoodFactsETL:
    def __init__(self, config_path='config.yaml'):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.spark = self._create_spark_session()
        self.metrics = {'timestamp': datetime.now().isoformat()}
    
    def _create_spark_session(self):
        return SparkSession.builder \
            .appName(self.config['spark']['app_name']) \
            .config("spark.driver.memory", self.config['spark']['driver_memory']) \
            .getOrCreate()
    
    def extract_bronze(self, path):
        """Phase BRONZE: Extraction brute"""
        print("=== BRONZE: Extraction ===")
        df = self.spark.read.json(path)
        self.metrics['products_read'] = df.count()
        return df
    
    def transform_silver(self, df):
        """Phase SILVER: Nettoyage et normalisation"""
        print("=== SILVER: Transformation ===")
        
        # Nettoyage basique
        df = df.filter(col("code").isNotNull())
        df = df.withColumn("product_name", 
            coalesce(col("product_name_fr"), col("product_name")))
        
        self.metrics['products_filtered'] = df.count()
        return df
    
    def load_gold(self, df):
        """Phase GOLD: Chargement datamart"""
        print("=== GOLD: Chargement ===")
        
        jdbc_url = self.config['mysql']['jdbc_url']
        props = {
            "user": self.config['mysql']['user'],
            "password": self.config['mysql']['password'],
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        
        # Exemple de chargement (simplifié)
        print("Chargement dans MySQL...")
        
    def run(self, source_path):
        """Exécute le pipeline complet"""
        print("\n=== DÉMARRAGE ETL ===\n")
        
        df_bronze = self.extract_bronze(source_path)
        df_silver = self.transform_silver(df_bronze)
        self.load_gold(df_silver)
        
        print("\n=== ETL TERMINÉ ===")
        print(f"Produits traités: {self.metrics['products_filtered']}")

if __name__ == "__main__":
    etl = OpenFoodFactsETL()
    etl.run("data/raw/openfoodfacts-sample.jsonl")
