"""
ETL OpenFoodFacts - Orchestrateur Principal
Version corrigée pour Windows (Spark + Hadoop winutils)
"""

import os
import sys

# ======================================================
# FIX SPARK / HADOOP SOUS WINDOWS
# ======================================================
HADOOP_HOME = "C:/hadoop"
SPARK_WAREHOUSE = "file:/C:/tmp/spark-warehouse"

os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["hadoop.home.dir"] = HADOOP_HOME
os.environ["spark.sql.warehouse.dir"] = SPARK_WAREHOUSE

os.makedirs("C:/hadoop/bin", exist_ok=True)
os.makedirs("C:/tmp/spark-warehouse", exist_ok=True)

# Python utilisé par Spark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
# ======================================================

from pyspark.sql import SparkSession
import yaml
import json
import logging
from datetime import datetime
from pathlib import Path

from bronze import BronzeExtractor
from silver import SilverTransformer
from gold import GoldLoader


class OpenFoodFactsETL:
    """Orchestrateur ETL"""

    def __init__(self, config_path="config.yaml"):
        self._setup_logging()
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        self.metrics = {
            "run_timestamp": datetime.now().isoformat(),
            "status": "started"
        }

    # --------------------------------------------------
    # LOGGING
    # --------------------------------------------------
    def _setup_logging(self):
        Path("logs").mkdir(exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("logs/etl.log", encoding="utf-8"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    # --------------------------------------------------
    # CONFIG
    # --------------------------------------------------
    def _load_config(self, config_path):
        local = Path("config.local.yaml")
        if local.exists():
            config_path = "config.local.yaml"
        self.logger.info(f"Chargement config depuis: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    # --------------------------------------------------
    # SPARK
    # --------------------------------------------------
    def _create_spark_session(self):
        builder = (
            SparkSession.builder
            .appName(self.config["spark"]["app_name"])
            .master(self.config["spark"]["master"])
            .config("spark.driver.memory", self.config["spark"]["driver_memory"])
            .config("spark.ui.showConsoleProgress", "false")
        )

        mysql_jar = self.config["spark"].get("mysql_jar", "")
        if mysql_jar and Path(mysql_jar).exists():
            jar_path = f"file:///{mysql_jar}" if not mysql_jar.startswith("file:") else mysql_jar
            builder = builder.config("spark.jars", jar_path)
            print(f"MySQL JAR chargé: {mysql_jar}")
        else:
            print("MySQL JAR non chargé")

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark

    # --------------------------------------------------
    # RUN
    # --------------------------------------------------
    def run(self):
        self.logger.info("\n" + "=" * 60)
        self.logger.info("DEMARRAGE ETL OPENFOODFACTS")
        self.logger.info("=" * 60)

        start = datetime.now()

        try:
            # -------- BRONZE --------
            self.logger.info("\n>>> PHASE BRONZE")
            extractor = BronzeExtractor(self.spark, self.config)
            df_bronze = extractor.extract()
            self.metrics["bronze"] = extractor.get_metrics()

            # -------- SILVER --------
            self.logger.info("\n>>> PHASE SILVER")
            transformer = SilverTransformer(self.spark, self.config)
            df_silver = transformer.transform(df_bronze)
            self.metrics["silver"] = transformer.get_metrics()

            # -------- GOLD --------
            self.logger.info("\n>>> PHASE GOLD")
            loader = GoldLoader(self.spark, self.config)
            loader.load(df_silver)
            self.metrics["gold"] = loader.get_metrics()

            self.metrics["status"] = "success"
            self.logger.info("ETL TERMINE AVEC SUCCES")

        except Exception as e:
            self.logger.error(f"ERREUR ETL: {e}")
            self.metrics["status"] = "failed"
            self.metrics["error"] = str(e)
            import traceback
            traceback.print_exc()

        finally:
            duration = (datetime.now() - start).total_seconds()
            self.metrics["duration_seconds"] = duration

            output_dir = Path("output/metrics")
            output_dir.mkdir(parents=True, exist_ok=True)
            metrics_file = output_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

            with open(metrics_file, "w", encoding="utf-8") as f:
                json.dump(self.metrics, f, indent=2, ensure_ascii=False)

            self.logger.info("\n" + "=" * 60)
            self.logger.info(f"Duree: {duration:.2f}s")
            self.logger.info(f"Statut: {self.metrics['status']}")
            self.logger.info(f"Metriques: {metrics_file}")
            self.logger.info("=" * 60)

            try:
                self.spark.stop()
            except Exception:
                pass


if __name__ == "__main__":
    etl = OpenFoodFactsETL()
    etl.run()
