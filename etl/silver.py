"""
PHASE SILVER - Nettoyage, normalisation et qualité des données
Conforme TRDE703 - Spark ETL
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, lower, when, lit, length, row_number,
    desc, avg, array, expr, size, regexp_replace,
    try_to_timestamp, sha2, concat_ws
)
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)


class SilverTransformer:
    """Transformateur Silver - Qualité, dédoublonnage, complétude"""

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.metrics = {}

    # ==================================================
    # NETTOYAGE & NORMALISATION
    # ==================================================
    def clean_and_normalize(self, df: DataFrame) -> DataFrame:
        logger.info("=== PHASE SILVER: Nettoyage et normalisation ===")

        df = (
            df.select(
                trim(col("code")).alias("code"),
                trim(col("product_name")).alias("product_name"),
                lower(trim(col("brands"))).alias("brands"),
                lower(trim(col("categories"))).alias("categories"),
                lower(trim(col("countries"))).alias("countries"),

                try_to_timestamp(col("last_modified_t")).alias("last_modified_ts"),

                lower(trim(col("nutriscore_grade"))).alias("nutriscore_grade"),

                regexp_replace(col("energy-kcal_100g"), ",", ".").cast(DoubleType()).alias("energy_kcal_100g"),
                regexp_replace(col("fat_100g"), ",", ".").cast(DoubleType()).alias("fat_100g"),
                regexp_replace(col("sugars_100g"), ",", ".").cast(DoubleType()).alias("sugars_100g"),
                regexp_replace(col("proteins_100g"), ",", ".").cast(DoubleType()).alias("proteins_100g"),
                regexp_replace(col("salt_100g"), ",", ".").cast(DoubleType()).alias("salt_100g"),
            )
        )

        logger.info("OK Nettoyage et normalisation effectues")
        return df

    # ==================================================
    # FILTRAGE QUALITÉ
    # ==================================================
    def filter_invalid_records(self, df: DataFrame) -> DataFrame:
        logger.info("Filtrage des enregistrements invalides...")

        before = df.count()

        df = (
            df.filter(col("code").isNotNull())
              .filter(length(col("code")) >= 8)
              .filter(col("code").rlike("^[0-9]+$"))
              .filter(col("last_modified_ts").isNotNull())
        )

        after = df.count()

        self.metrics.update({
            "products_input": before,
            "products_filtered": after,
            "products_rejected": before - after,
            "rejection_rate_pct": round(((before - after) / before * 100), 2) if before else 0.0
        })

        logger.info("OK Filtrage: %s produits conserves", after)
        return df

    # ==================================================
    # DÉDOUBLONNAGE
    # ==================================================
    def deduplicate(self, df: DataFrame) -> DataFrame:
        logger.info("Dedoublonnage par code + last_modified_ts")

        before = df.count()

        w = Window.partitionBy("code").orderBy(desc("last_modified_ts"))
        df = (
            df.withColumn("rn", row_number().over(w))
              .filter(col("rn") == 1)
              .drop("rn")
        )

        self.metrics["duplicates_removed"] = before - df.count()
        return df

    # ==================================================
    # COMPLÉTUDE
    # ==================================================
    def calculate_completeness(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "completeness_score",
            (
                when(col("product_name").isNotNull(), 1).otherwise(0) +
                when(col("brands").isNotNull(), 1).otherwise(0) +
                when(col("energy_kcal_100g").isNotNull(), 1).otherwise(0) +
                when(col("fat_100g").isNotNull(), 1).otherwise(0) +
                when(col("sugars_100g").isNotNull(), 1).otherwise(0)
            ) / lit(5.0)
        )

    # ==================================================
    # ANOMALIES
    # ==================================================
    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "quality_issues",
            expr("""
                filter(array(
                    CASE WHEN energy_kcal_100g > 900 THEN 'energy_out_of_bounds' END,
                    CASE WHEN energy_kcal_100g < 0 THEN 'negative_energy' END,
                    CASE WHEN sugars_100g > 100 THEN 'sugars_out_of_bounds' END,
                    CASE WHEN sugars_100g < 0 THEN 'negative_sugars' END,
                    CASE WHEN fat_100g > 100 THEN 'fat_out_of_bounds' END,
                    CASE WHEN proteins_100g > 100 THEN 'proteins_out_of_bounds' END,
                    CASE WHEN salt_100g > 25 THEN 'salt_out_of_bounds' END,
                    CASE WHEN salt_100g < 0 THEN 'negative_salt' END
                ), x -> x IS NOT NULL)
            """)
        )

    # ==================================================
    # HASH PRODUIT (SCD2)
    # ==================================================
    def compute_product_hash(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "product_hash",
            sha2(
                concat_ws(
                    "||",
                    "product_name",
                    "brands",
                    "categories",
                    "countries",
                    "nutriscore_grade"
                ),
                256
            )
        )

    # ==================================================
    # PIPELINE
    # ==================================================
    def transform(self, df_bronze: DataFrame) -> DataFrame:
        logger.info("=== DEMARRAGE PHASE SILVER ===")

        df = self.clean_and_normalize(df_bronze)
        df = self.filter_invalid_records(df)
        df = self.deduplicate(df)
        df = self.calculate_completeness(df)
        df = self.detect_anomalies(df)
        df = self.compute_product_hash(df)

        final_count = df.count()
        avg_comp = df.agg(avg("completeness_score")).first()[0] or 0.0
        anomalies = df.filter(size(col("quality_issues")) > 0).count()

        self.metrics.update({
            "products_final": final_count,
            "avg_completeness": round(avg_comp, 4),
            "completeness_pct": round(avg_comp * 100, 2),
            "anomalies_count": anomalies
        })

        df.cache()
        logger.info("=== PHASE SILVER TERMINEE ===")
        return df

    def get_metrics(self) -> dict:
        return self.metrics
