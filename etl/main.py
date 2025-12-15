import argparse
import json
import logging
import os
from datetime import datetime

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from pyspark.sql.functions import col, trim, lower, to_timestamp

from etl.gold import GoldLoader

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("etl")


# ============================================================
# CONFIG
# ============================================================

def load_config(path: str) -> dict:
    logger.info("Chargement config depuis: %s", path)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ============================================================
# SPARK SESSION (FIX JDBC WINDOWS)
# ============================================================

def build_spark(cfg: dict) -> SparkSession:
    spark_cfg = cfg.get("spark", {})

    app_name = spark_cfg.get("app_name", "OpenFoodFacts ETL")
    master = spark_cfg.get("master", "local[*]")
    driver_memory = spark_cfg.get("driver_memory", "4g")
    executor_memory = spark_cfg.get("executor_memory", "4g")

    mysql_jar = spark_cfg.get("jars")  # file:///C:/spark/jars/mysql-connector-xxx.jar

    if not mysql_jar:
        raise ValueError("spark.jars manquant dans la configuration")

    # Spark a besoin du chemin LOCAL pour le classpath JVM
    mysql_jar_local = mysql_jar.replace("file:///", "")

    logger.info("MySQL JAR utilisé: %s", mysql_jar)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "Europe/Paris")
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)

        # 🔥 FIX CRITIQUE JDBC WINDOWS
        .config("spark.jars", mysql_jar)
        .config("spark.driver.extraClassPath", mysql_jar_local)
        .config("spark.executor.extraClassPath", mysql_jar_local)

        .getOrCreate()
    )

    return spark


# ============================================================
# PIPELINE
# ============================================================

def run_pipeline(cfg: dict):
    spark = build_spark(cfg)

    start = datetime.now()
    status = "success"
    metrics = {}

    try:
        logger.info("\n============================================================")
        logger.info("DEMARRAGE ETL OPENFOODFACTS")
        logger.info("============================================================\n")

        # ====================================================
        # BRONZE
        # ====================================================
        logger.info(">>> PHASE BRONZE")
        logger.info("=== PHASE BRONZE: Extraction CSV ===")

        off = cfg.get("openfoodfacts", {})
        raw_path = off.get("raw_data_path")
        delimiter = off.get("delimiter", "\t")
        header = bool(off.get("header", True))

        logger.info("Source: %s", raw_path)

        schema = StructType([
            StructField("code", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("brands", StringType(), True),
            StructField("categories", StringType(), True),
            StructField("countries", StringType(), True),
            StructField("energy-kcal_100g", StringType(), True),
            StructField("fat_100g", StringType(), True),
            StructField("sugars_100g", StringType(), True),
            StructField("proteins_100g", StringType(), True),
            StructField("salt_100g", StringType(), True),
            StructField("nutriscore_grade", StringType(), True),
            StructField("last_modified_t", StringType(), True),
        ])

        df_bronze = (
            spark.read
            .format("csv")
            .option("header", str(header).lower())
            .option("sep", delimiter)
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .load(raw_path)
        )

        count_bronze = df_bronze.count()
        logger.info("✓ %d produits extraits", count_bronze)
        logger.info("Colonnes lues: %d", len(df_bronze.columns))

        # ====================================================
        # SILVER
        # ====================================================
        logger.info("\n>>> PHASE SILVER")
        logger.info("============================================================")
        logger.info("=== DEMARRAGE PHASE SILVER ===")
        logger.info("============================================================")
        logger.info("=== PHASE SILVER: Nettoyage et normalisation ===")

        df_silver = (
            df_bronze
            .select(
                trim(col("code")).alias("code"),
                trim(col("product_name")).alias("product_name"),
                trim(col("brands")).alias("brands"),
                trim(col("categories")).alias("categories"),
                trim(col("countries")).alias("countries"),
                to_timestamp(col("last_modified_t")).alias("last_modified_ts"),
                col("energy-kcal_100g").cast(DoubleType()).alias("energy_kcal_100g"),
                col("fat_100g").cast(DoubleType()).alias("fat_100g"),
                col("sugars_100g").cast(DoubleType()).alias("sugars_100g"),
                col("proteins_100g").cast(DoubleType()).alias("proteins_100g"),
                col("salt_100g").cast(DoubleType()).alias("salt_100g"),
                lower(trim(col("nutriscore_grade"))).alias("nutriscore_grade"),
            )
        )

        logger.info("OK Nettoyage et normalisation effectues")

        logger.info("Filtrage des enregistrements invalides...")
        df_silver = df_silver.filter(
            col("code").isNotNull()
            & col("code").rlike("^[0-9]+$")
        )

        count_silver = df_silver.count()
        logger.info("OK Filtrage: %d produits conserves", count_silver)

        metrics["bronze_count"] = count_bronze
        metrics["silver_count"] = count_silver

        logger.info("============================================================")
        logger.info("=== PHASE SILVER TERMINEE ===")
        logger.info("Produits finaux: %d", count_silver)
        logger.info("============================================================")

        # ====================================================
        # GOLD
        # ====================================================
        logger.info("\n>>> PHASE GOLD")
        loader = GoldLoader(spark, cfg)
        loader.load(df_silver)

    except Exception as e:
        status = "failed"
        logger.exception("ERREUR ETL")
        raise e

    finally:
        duration = (datetime.now() - start).total_seconds()
        metrics["duration_s"] = duration
        metrics["status"] = status

        os.makedirs("output/metrics", exist_ok=True)
        out = f"output/metrics/run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, ensure_ascii=False)

        logger.info("\n============================================================")
        logger.info("Duree: %.2fs", duration)
        logger.info("Statut: %s", status)
        logger.info("Metriques: %s", out)
        logger.info("============================================================")

        try:
            spark.stop()
        except Exception:
            pass


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        required=True,
        help="Chemin du fichier YAML de configuration"
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    run_pipeline(cfg)


if __name__ == "__main__":
    main()
