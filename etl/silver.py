"""
PHASE SILVER - Nettoyage, normalisation et qualité des données
Conforme TRDE703 - Spark ETL
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, lower, when, lit, length, row_number,
    desc, avg, array, expr, size, array_contains,
    to_timestamp, sha2, concat_ws
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

        df = df.select(
            trim(col("code")).alias("code"),
            trim(col("product_name")).alias("product_name"),
            trim(col("brands")).alias("brands"),
            trim(col("categories")).alias("categories"),
            trim(col("countries")).alias("countries"),

            to_timestamp(col("last_modified_t")).alias("last_modified_ts"),

            lower(trim(col("nutriscore_grade"))).alias("nutriscore_grade"),

            col("energy_kcal_100g").cast(DoubleType()),
            col("fat_100g").cast(DoubleType()),
            col("sugars_100g").cast(DoubleType()),
            col("proteins_100g").cast(DoubleType()),
            col("salt_100g").cast(DoubleType())
        )

        logger.info("OK Nettoyage et normalisation effectues")
        return df

    # ==================================================
    # FILTRAGE QUALITÉ MINIMALE
    # ==================================================
    def filter_invalid_records(self, df: DataFrame) -> DataFrame:
        logger.info("Filtrage des enregistrements invalides...")

        before = df.count()

        df = df.filter(
            col("code").isNotNull() &
            (length(col("code")) >= 8) &
            col("code").rlike("^[0-9]+$")
        )

        after = df.count()

        self.metrics["products_input"] = before
        self.metrics["products_filtered"] = after
        self.metrics["products_rejected"] = before - after
        self.metrics["rejection_rate_pct"] = round(
            ((before - after) / before * 100), 2
        ) if before > 0 else 0.0

        logger.info(f"OK Filtrage: {after} produits conserves")
        return df

    # ==================================================
    # DÉDOUBLONNAGE (DERNIÈRE MODIF)
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

        after = df.count()
        self.metrics["duplicates_removed"] = before - after

        logger.info(f"OK Dedoublonnage: {before - after} doublons supprimes")
        return df

    # ==================================================
    # COMPLÉTUDE
    # ==================================================
    def calculate_completeness(self, df: DataFrame) -> DataFrame:
        logger.info("Calcul du score de completude")

        df = df.withColumn(
            "completeness_score",
            (
                when(col("product_name").isNotNull(), 1).otherwise(0) +
                when(col("brands").isNotNull(), 1).otherwise(0) +
                when(col("energy_kcal_100g").isNotNull(), 1).otherwise(0) +
                when(col("fat_100g").isNotNull(), 1).otherwise(0) +
                when(col("sugars_100g").isNotNull(), 1).otherwise(0)
            ) / lit(5.0)
        )

        return df

    # ==================================================
    # DÉTECTION DES ANOMALIES
    # ==================================================
    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        logger.info("Detection des anomalies nutritionnelles")

        df = df.withColumn(
            "quality_issues",
            array(
                when(col("energy_kcal_100g") > 900, "energy_out_of_bounds"),
                when(col("energy_kcal_100g") < 0, "negative_energy"),
                when(col("sugars_100g") > 100, "sugars_out_of_bounds"),
                when(col("sugars_100g") < 0, "negative_sugars"),
                when(col("fat_100g") > 100, "fat_out_of_bounds"),
                when(col("proteins_100g") > 100, "proteins_out_of_bounds"),
                when(col("salt_100g") > 25, "salt_out_of_bounds"),
                when(col("salt_100g") < 0, "negative_salt")
            )
        )

        df = df.withColumn(
            "quality_issues",
            expr("filter(quality_issues, x -> x is not null)")
        )

        return df

    # ==================================================
    # HASH PRODUIT (SCD2)
    # ==================================================
    def compute_product_hash(self, df: DataFrame) -> DataFrame:
        logger.info("Calcul du hash produit (SCD2 ready)")

        df = df.withColumn(
            "product_hash",
            sha2(
                concat_ws(
                    "||",
                    col("product_name"),
                    col("brands"),
                    col("categories"),
                    col("countries"),
                    col("nutriscore_grade")
                ),
                256
            )
        )

        return df

    # ==================================================
    # PIPELINE COMPLET
    # ==================================================
    def transform(self, df_bronze: DataFrame) -> DataFrame:
        logger.info("=" * 60)
        logger.info("=== DEMARRAGE PHASE SILVER ===")
        logger.info("=" * 60)

        df = df_bronze
        df = self.clean_and_normalize(df)
        df = self.filter_invalid_records(df)
        df = self.deduplicate(df)
        df = self.calculate_completeness(df)
        df = self.detect_anomalies(df)
        df = self.compute_product_hash(df)

        final_count = df.count()

        avg_comp = df.agg(avg("completeness_score")).first()[0] or 0.0
        anomalies = df.filter(size(col("quality_issues")) > 0).count()

        self.metrics["avg_completeness"] = round(avg_comp, 4)
        self.metrics["completeness_pct"] = round(avg_comp * 100, 2)
        self.metrics["anomalies"] = {
            "total_products_with_anomalies": anomalies,
            "anomaly_rate_pct": round((anomalies / final_count * 100), 2)
            if final_count > 0 else 0.0
        }

        df.cache()

        logger.info("=" * 60)
        logger.info("=== PHASE SILVER TERMINEE ===")
        logger.info(f"Produits finaux: {final_count}")
        logger.info(f"Completude moyenne: {self.metrics['completeness_pct']}%")
        logger.info("=" * 60)

        return df

    def get_metrics(self) -> dict:
        return self.metrics
