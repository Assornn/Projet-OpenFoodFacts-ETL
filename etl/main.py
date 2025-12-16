"""
Main entry point for OpenFoodFacts ETL
Orchestrator: Bronze -> Silver -> Gold
"""

import argparse
import logging
import yaml
import sys
from pathlib import Path

from pyspark.sql import SparkSession

# Import des modules locaux
try:
    from etl.bronze import BronzeExtractor
    from etl.silver import SilverTransformer
    from etl.gold import GoldLoader
except ImportError:
    from bronze import BronzeExtractor
    from silver import SilverTransformer
    from gold import GoldLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """Charge la configuration YAML avec encodage UTF-8 forcé et sécurité None."""
    logger.info(f"Chargement config depuis: {config_path}")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            # CORRECTION : Si le fichier est vide, yaml retourne None. On force {}.
            return data if data is not None else {}
    except Exception as e:
        logger.error(f"Erreur chargement config: {e}")
        sys.exit(1)


def create_spark_session(app_name: str, config: dict) -> SparkSession:
    """Initialise la session Spark."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.debug.maxToStringFields", "100")
        .config("spark.sql.session.timeZone", "UTC")
    )
    
    if "spark" in config and config["spark"] and "jars" in config["spark"]:
        builder = builder.config("spark.jars", config["spark"]["jars"])

    return builder.getOrCreate()


def run_pipeline(config_path: str):
    """Exécute le pipeline ETL complet."""
    config = load_config(config_path)
    spark = create_spark_session("OpenFoodFacts ETL", config)
    
    try:
        # BRONZE
        extractor = BronzeExtractor(spark, config)
        df_bronze = extractor.extract()
        
        # SILVER
        transformer = SilverTransformer(spark, config)
        df_silver = transformer.transform(df_bronze)
        
        # GOLD
        loader = GoldLoader(spark, config)
        loader.load(df_silver)
        
        metrics = {
            "bronze": extractor.get_metrics(),
            "silver": transformer.get_metrics()
        }
        logger.info("=== SUCCES PIPELINE ===")
        logger.info(f"Metriques: {metrics}")

    except Exception as e:
        logger.error("=== ECHEC PIPELINE ===")
        logger.exception(e)
        raise e
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="ETL OpenFoodFacts")
    parser.add_argument("--config", required=True, help="Chemin vers le fichier config.yaml")
    args = parser.parse_args()
    run_pipeline(args.config)


if __name__ == "__main__":
    main()