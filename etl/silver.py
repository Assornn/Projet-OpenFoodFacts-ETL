"""
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
