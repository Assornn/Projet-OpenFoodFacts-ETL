"""
Main entry point for OpenFoodFacts ETL
Orchestrator: Bronze -> Silver -> Gold
"""

import argparse
import logging
import yaml
import sys
import json  # <--- Ajout pour sauvegarder les métriques
from pathlib import Path

from pyspark.sql import SparkSession

# Import des modules locaux
# Le try/except permet de gérer l'exécution locale vs l'exécution via spark-submit --py-files
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
    """Charge la configuration YAML avec encodage UTF-8 forcé."""
    logger.info(f"Chargement config depuis: {config_path}")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            # Sécurité : Si le fichier est vide ou mal lu, on retourne un dict vide pour éviter le crash NoneType
            return data if data is not None else {}
    except Exception as e:
        logger.error(f"Erreur chargement config: {e}")
        sys.exit(1)


def create_spark_session(app_name: str, config: dict) -> SparkSession:
    """Initialise la session Spark avec les paramètres optimisés."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        # Augmente la tolérance pour les plans d'exécution complexes (utile pour 4M lignes)
        .config("spark.sql.debug.maxToStringFields", "1000") 
        .config("spark.sql.session.timeZone", "UTC")
    )
    
    # Ajout dynamique des Jars si présents dans la config
    if "spark" in config and config["spark"] and "jars" in config["spark"]:
        builder = builder.config("spark.jars", config["spark"]["jars"])

    return builder.getOrCreate()


def run_pipeline(config_path: str):
    """Exécute le pipeline ETL complet et sauvegarde les métriques."""
    config = load_config(config_path)
    spark = create_spark_session("OpenFoodFacts ETL", config)
    
    try:
        # --- 1. BRONZE (Extraction) ---
        extractor = BronzeExtractor(spark, config)
        df_bronze = extractor.extract()
        
        # --- 2. SILVER (Transformation) ---
        transformer = SilverTransformer(spark, config)
        df_silver = transformer.transform(df_bronze)
        
        # --- 3. GOLD (Chargement) ---
        loader = GoldLoader(spark, config)
        loader.load(df_silver)
        
        # --- 4. METRIQUES & REPORTING ---
        metrics = {
            "bronze": extractor.get_metrics(),
            "silver": transformer.get_metrics()
        }
        
        # Sauvegarde des métriques dans un fichier JSON (Exigence TP)
        output_file = "metrics_last_run.json"
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(metrics, f, indent=4)
            logger.info(f"📄 Metriques sauvegardees dans '{output_file}'")
        except Exception as e:
            logger.warning(f"Impossible d'ecrire le fichier JSON: {e}")

        logger.info("=== SUCCES PIPELINE ===")
        logger.info(f"Metriques: {metrics}")

        return metrics

    except Exception as e:
        logger.error("=== ECHEC PIPELINE ===")
        # On loggue l'erreur sans emojis pour éviter les crashs Windows
        logger.error(str(e))
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