"""
PHASE SILVER - Nettoyage, normalisation et qualité des données
Conforme TRDE703 - Spark ETL
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, lower, when, lit, length, row_number,
    desc, avg, array, expr, size, regexp_replace,
    sha2, concat_ws, from_unixtime, to_timestamp, coalesce, array_union
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

        # BONUS MULTILINGUE : Résolution intelligente du nom
        # On vérifie si les colonnes _fr existent, sinon on fallback sur product_name
        name_col = col("product_name")
        if "product_name_fr" in df.columns and "product_name_en" in df.columns:
            name_col = coalesce(col("product_name_fr"), col("product_name_en"), col("product_name"))
        
        df = (
            df.select(
                # Textes : Trim + Lower
                trim(col("code")).alias("code"),
                
                # Bonus Multilingue appliqué ici
                trim(name_col).alias("product_name"),
                
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
    # ANOMALIES (RÈGLES MÉTIER)
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
    # BONUS : ANOMALIES STATISTIQUES (IQR)
    # ==================================================
    def detect_outliers_iqr(self, df: DataFrame) -> DataFrame:
        """
        Détecte les valeurs aberrantes statistiquement (Interquartile Range).
        Ajoute ces anomalies dans la liste 'quality_issues' existante.
        """
        logger.info("Detection des anomalies statistiques (IQR)...")
        
        # Liste des colonnes numériques à vérifier
        numeric_cols = ["energy_kcal_100g", "fat_100g", "sugars_100g", "salt_100g", "proteins_100g"]
        
        # Sécurité : Si DF vide, on return direct
        try:
            if df.rdd.isEmpty(): return df
        except:
            return df

        bounds_sql_parts = []
        
        for col_name in numeric_cols:
            try:
                # Calcul Q1 (25%) et Q3 (75%) avec une erreur relative de 5% (pour la perf)
                quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.05)
                q1, q3 = quantiles[0], quantiles[1]
                
                # Calcul IQR et bornes
                iqr = q3 - q1
                # On ne vérifie que la borne haute (Upper Fence) pour les nutriments
                upper_bound = q3 + 1.5 * iqr
                
                # Si l'écart est nul (toutes les valeurs sont identiques), on ignore
                if iqr == 0: continue

                # Ajout de la règle SQL
                bounds_sql_parts.append(
                    f"CASE WHEN {col_name} > {upper_bound} THEN '{col_name}_stat_high' END"
                )
                logger.info(f"IQR {col_name}: Seuil Statistique = {upper_bound:.2f}")
                
            except Exception as e:
                logger.warning(f"Skip IQR pour {col_name}: {e}")

        if not bounds_sql_parts:
            return df

        # Création d'un tableau temporaire d'anomalies IQR
        iqr_expr = "filter(array(" + ", ".join(bounds_sql_parts) + "), x -> x IS NOT NULL)"
        
        # Fusion intelligente avec le tableau existant
        return df.withColumn("iqr_issues", expr(iqr_expr)) \
                 .withColumn("quality_issues", array_union(col("quality_issues"), col("iqr_issues"))) \
                 .drop("iqr_issues")

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
        
        # Détection anomalies métier
        df = self.detect_anomalies(df)
        
        # Détection anomalies statistiques (BONUS)
        df = self.detect_outliers_iqr(df)
        
        df = self.compute_product_hash(df)

        df.cache()
        
        final_count = df.count()
        row_avg = df.agg(avg("completeness_score")).first()
        avg_comp = row_avg[0] if row_avg and row_avg[0] else 0.0
        
        # On compte les lignes où quality_issues n'est pas vide
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