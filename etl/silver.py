"""
PHASE SILVER - Nettoyage, normalisation et qualité des données
Conforme TRDE703 - Spark ETL
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, lower, when, lit, length, row_number,
    desc, avg, array, expr, size, regexp_replace,
    sha2, concat_ws, from_unixtime, to_timestamp, coalesce
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

        # Note: On utilise expr("try_cast(...)") pour être compatible
        # avec les versions de PySpark où la fonction python try_cast n'existe pas encore.
        
        df = (
            df.select(
                # Textes : Trim + Lower
                trim(col("code")).alias("code"),
                trim(col("product_name")).alias("product_name"),
                lower(trim(col("brands"))).alias("brands"),
                lower(trim(col("categories"))).alias("categories"),
                lower(trim(col("countries"))).alias("countries"),

                # Timestamp : Gestion duale via expressions SQL
                coalesce(
                    # Cas 1 : Format Unix (chiffres)
                    to_timestamp(from_unixtime(expr("try_cast(last_modified_t AS BIGINT)"))),
                    # Cas 2 : Format ISO String
                    expr("try_cast(last_modified_t AS TIMESTAMP)")
                ).alias("last_modified_ts"),

                lower(trim(col("nutriscore_grade"))).alias("nutriscore_grade"),

                # Numeriques : Utilisation de SQL expr pour try_cast
                # Attention aux backticks pour energy-kcal qui a un tiret
                expr("try_cast(replace(`energy-kcal_100g`, ',', '.') AS DOUBLE)").alias("energy_kcal_100g"),
                expr("try_cast(replace(fat_100g, ',', '.') AS DOUBLE)").alias("fat_100g"),
                expr("try_cast(replace(sugars_100g, ',', '.') AS DOUBLE)").alias("sugars_100g"),
                expr("try_cast(replace(proteins_100g, ',', '.') AS DOUBLE)").alias("proteins_100g"),
                expr("try_cast(replace(salt_100g, ',', '.') AS DOUBLE)").alias("salt_100g"),
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
        )

        after = df.count()

        self.metrics.update({
            "products_input": before,
            "products_filtered": after,
            "products_rejected": before - after,
            "rejection_rate_pct": round(((before - after) / before * 100), 2) if before else 0.0
        })

        logger.info(f"OK Filtrage: {after} produits conserves")
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
                    CASE WHEN energy_kcal_100g > 9000 THEN 'energy_out_of_bounds' END,
                    CASE WHEN energy_kcal_100g < 0 THEN 'negative_energy' END,
                    CASE WHEN sugars_100g > 100 THEN 'sugars_out_of_bounds' END,
                    CASE WHEN sugars_100g < 0 THEN 'negative_sugars' END,
                    CASE WHEN fat_100g > 100 THEN 'fat_out_of_bounds' END,
                    CASE WHEN proteins_100g > 100 THEN 'proteins_out_of_bounds' END,
                    CASE WHEN salt_100g > 100 THEN 'salt_out_of_bounds' END,
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
                    col("product_name"),
                    col("brands"),
                    col("categories"),
                    col("countries"),
                    col("nutriscore_grade"),
                    col("energy_kcal_100g").cast("string"),
                    col("sugars_100g").cast("string")
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

        df.cache()
        
        final_count = df.count()
        row_avg = df.agg(avg("completeness_score")).first()
        avg_comp = row_avg[0] if row_avg and row_avg[0] else 0.0
        
        anomalies = df.filter(size(col("quality_issues")) > 0).count()

        self.metrics.update({
            "products_final": final_count,
            "avg_completeness": round(avg_comp, 4),
            "completeness_pct": round(avg_comp * 100, 2),
            "anomalies_count": anomalies
        })

        logger.info(f"=== PHASE SILVER TERMINEE (Produits: {final_count}) ===")
        return df

    def get_metrics(self) -> dict:
        return self.metrics