"""
PHASE BRONZE - Extraction des données OpenFoodFacts (CSV/TSV)
Conforme TRDE703 - Spark ETL
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class BronzeExtractor:
    """Extracteur Bronze - Lecture OpenFoodFacts CSV avec schéma explicite"""

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.metrics = {}

    # ==================================================
    # SCHÉMA EXPLICITE (OBLIGATOIRE EN PROD)
    # ==================================================
    def _get_schema(self) -> StructType:
        """
        Schéma explicite OpenFoodFacts (subset utile)
        Évite inferSchema (interdit en prod)
        """
        return StructType([
            StructField("code", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("brands", StringType(), True),
            StructField("categories", StringType(), True),
            StructField("countries", StringType(), True),
            StructField("last_modified_t", StringType(), True),

            StructField("energy_kcal_100g", DoubleType(), True),
            StructField("fat_100g", DoubleType(), True),
            StructField("sugars_100g", DoubleType(), True),
            StructField("proteins_100g", DoubleType(), True),
            StructField("salt_100g", DoubleType(), True),

            StructField("nutriscore_grade", StringType(), True)
        ])

    # ==================================================
    # EXTRACTION CSV
    # ==================================================
    def extract_csv(self, source_path: str) -> DataFrame:
        """
        Extraction CSV OpenFoodFacts (TSV)
        """
        logger.info("=== PHASE BRONZE: Extraction CSV ===")
        logger.info(f"Source: {source_path}")

        if not Path(source_path).exists():
            raise FileNotFoundError(f"Fichier introuvable: {source_path}")

        try:
            df = (
                self.spark.read
                .format("csv")
                .option("header", True)
                .option("sep", "\t")
                .option("quote", '"')
                .option("escape", "\\")
                .option("multiLine", True)
                .schema(self._get_schema())   # 🚨 schéma explicite
                .load(source_path)
            )

            count = df.count()

            # ======================
            # MÉTRIQUES
            # ======================
            self.metrics = {
                "products_read": count,
                "source_path": source_path,
                "columns_count": len(df.columns),
                "columns": df.columns
            }

            logger.info(f"✓ {count} produits extraits")
            logger.info(f"Colonnes lues: {len(df.columns)}")

            # Cache Bronze (réutilisé par Silver)
            df.cache()

            return df

        except Exception as e:
            logger.error(f"✗ Erreur extraction Bronze: {e}")
            raise

    # ==================================================
    # POINT D’ENTRÉE
    # ==================================================
    def extract(self, source_path: str = None) -> DataFrame:
        """
        Point d'entrée principal Bronze
        """
        if source_path is None:
            source_path = self.config["openfoodfacts"]["raw_data_path"]

        return self.extract_csv(source_path)

    # ==================================================
    # MÉTRIQUES
    # ==================================================
    def get_metrics(self) -> dict:
        return self.metrics
