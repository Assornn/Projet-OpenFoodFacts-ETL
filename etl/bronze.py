"""
PHASE BRONZE - Extraction des données OpenFoodFacts (CSV)
Conforme TRDE703 - Spark ETL
"""

import logging
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)


class BronzeExtractor:
    """
    Extracteur Bronze
    - Lecture CSV OpenFoodFacts (header réel)
    - Colonnes RAW en STRING
    - Aucune transformation métier
    """

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.metrics = {}

        # Limite optionnelle (tests locaux)
        self.limit_rows = (
            config.get("bronze", {})
                  .get("limit_rows")
        )

    # ==================================================
    # EXTRACTION CSV OPENFOODFACTS
    # ==================================================
    def extract_csv(self, source_path: str) -> DataFrame:
        logger.info("=== PHASE BRONZE: Extraction OpenFoodFacts ===")
        logger.info(f"Source: {source_path}")

        if not Path(source_path).exists():
            raise FileNotFoundError(f"Fichier introuvable: {source_path}")

        try:
            # --------------------------------------------------
            # LECTURE CSV ROBUSTE (HEADER RÉEL)
            # --------------------------------------------------
            df_raw = (
                self.spark.read
                .option("header", "true")
                .option("sep", ",")              # CSV OpenFoodFacts
                .option("quote", '"')
                .option("escape", '"')
                .option("multiLine", "true")
                .option("mode", "PERMISSIVE")
                .csv(source_path)
            )

            initial_count = df_raw.count()

            # --------------------------------------------------
            # SÉLECTION DES COLONNES UTILES (RAW)
            # --------------------------------------------------
            df = df_raw.select(
                col("code").cast("string"),
                col("product_name").cast("string"),
                col("brands").cast("string"),
                col("categories").cast("string"),
                col("countries").cast("string"),

                col("energy-kcal_100g").cast("string"),
                col("fat_100g").cast("string"),
                col("sugars_100g").cast("string"),
                col("proteins_100g").cast("string"),
                col("salt_100g").cast("string"),

                col("nutriscore_grade").cast("string"),
                col("completeness_score").cast("string"),
                col("last_modified_t").cast("string"),
            )

            # --------------------------------------------------
            # LIMITE OPTIONNELLE
            # --------------------------------------------------
            if self.limit_rows:
                logger.warning(
                    "[BRONZE] Limitation activée: %s lignes",
                    self.limit_rows
                )
                df = df.limit(int(self.limit_rows))

            final_count = df.count()

            # --------------------------------------------------
            # MÉTRIQUES
            # --------------------------------------------------
            self.metrics = {
                "products_read_raw": initial_count,
                "products_read": final_count,
                "limit_rows": self.limit_rows,
                "source_path": source_path,
                "columns_count": len(df.columns),
                "columns": df.columns,
            }

            logger.info(f"✓ Produits extraits: {final_count}")
            logger.info(f"Colonnes lues: {len(df.columns)}")

            df.cache()
            return df

        except Exception as e:
            logger.exception("✗ Erreur extraction Bronze")
            raise e

    # ==================================================
    # POINT D’ENTRÉE
    # ==================================================
    def extract(self, source_path: str = None) -> DataFrame:
        if source_path is None:
            source_path = self.config["openfoodfacts"]["raw_data_path"]

        return self.extract_csv(source_path)

    # ==================================================
    # MÉTRIQUES
    # ==================================================
    def get_metrics(self) -> dict:
        return self.metrics
