"""
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
