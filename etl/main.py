"""
ETL OpenFoodFacts - Orchestrateur Principal
"""

import os
import sys
from pyspark.sql import SparkSession
import yaml
import json
import logging
from datetime import datetime
from pathlib import Path

from bronze import BronzeExtractor
from silver import SilverTransformer
# from gold import GoldLoader  # Commenter temporairement


class OpenFoodFactsETL:
    """Orchestrateur ETL"""
    
    def __init__(self, config_path='config.yaml'):
        # Configurer l'environnement pour Windows
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        
        self._setup_logging()
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        self.metrics = {
            'run_timestamp': datetime.now().isoformat(),
            'status': 'started'
        }
    
    def _setup_logging(self):
        """Configure le système de logging"""
        Path('logs').mkdir(exist_ok=True)
        
        # Désactiver les emojis pour éviter les erreurs Unicode
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/etl.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _load_config(self, config_path):
        """Charge la configuration"""
        local = Path('config.local.yaml')
        if local.exists():
            config_path = 'config.local.yaml'
            self.logger.info(f"Chargement config depuis: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _create_spark_session(self):
        """Crée la session Spark"""
        builder = SparkSession.builder \
            .appName(self.config['spark']['app_name']) \
            .master(self.config['spark']['master']) \
            .config("spark.driver.memory", self.config['spark']['driver_memory']) \
            .config("spark.ui.showConsoleProgress", "false")
        
        # Gestion du JAR MySQL (optionnel)
        mysql_jar = self.config['spark'].get('mysql_jar', '')
        if mysql_jar and mysql_jar.strip() and Path(mysql_jar).exists():
            jar_path = f"file:///{mysql_jar}" if not mysql_jar.startswith('file:') else mysql_jar
            builder = builder.config("spark.jars", jar_path)
            print(f"MySQL JAR charge: {mysql_jar}")
        else:
            print("MySQL JAR non charge - phase Gold sera ignoree")
        
        return builder.getOrCreate()
    
    def run(self):
        """Exécute le pipeline complet"""
        self.logger.info("\n" + "="*60)
        self.logger.info("DEMARRAGE ETL OPENFOODFACTS")
        self.logger.info("="*60)
        
        start = datetime.now()
        
        try:
            # Bronze
            self.logger.info("\n>>> PHASE BRONZE: Extraction")
            extractor = BronzeExtractor(self.spark, self.config)
            df_bronze = extractor.extract()
            self.metrics['bronze'] = extractor.get_metrics()
            self.logger.info(f"Bronze OK - {self.metrics['bronze']['products_read']} produits extraits")
            
            # Silver
            self.logger.info("\n>>> PHASE SILVER: Transformation")
            transformer = SilverTransformer(self.spark, self.config)
            df_silver = transformer.transform(df_bronze)
            self.metrics['silver'] = transformer.get_metrics()
            self.logger.info(f"Silver OK - {self.metrics['silver']['products_filtered']} produits transformes")
            
            # Gold - COMMENTER SI PAS DE MYSQL
            # self.logger.info("\n>>> PHASE GOLD: Chargement")
            # loader = GoldLoader(self.spark, self.config)
            # loader.load(df_silver)
            # self.metrics['gold'] = loader.get_metrics()
            # self.logger.info(f"Gold OK - {self.metrics['gold'].get('fact_nutrition_loaded', 0)} faits charges")
            
            self.logger.info("\n>>> ETL TERMINE (Bronze + Silver)")
            self.metrics['status'] = 'success'
            
        except Exception as e:
            self.logger.error(f"\nERREUR: {e}")
            self.metrics['status'] = 'failed'
            self.metrics['error'] = str(e)
            import traceback
            traceback.print_exc()
        
        finally:
            duration = (datetime.now() - start).total_seconds()
            self.metrics['duration_seconds'] = duration
            
            # Sauvegarder métriques
            output_dir = Path('output/metrics')
            output_dir.mkdir(parents=True, exist_ok=True)
            
            metrics_file = output_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(metrics_file, 'w', encoding='utf-8') as f:
                json.dump(self.metrics, f, indent=2, ensure_ascii=False)
            
            # Afficher résumé
            self.logger.info("\n" + "="*60)
            self.logger.info(f"Duree: {duration:.2f}s")
            self.logger.info(f"Statut: {self.metrics['status']}")
            if 'bronze' in self.metrics:
                self.logger.info(f"Produits extraits: {self.metrics['bronze']['products_read']}")
            if 'silver' in self.metrics:
                self.logger.info(f"Produits traites: {self.metrics['silver']['products_filtered']}")
                if 'completeness_pct' in self.metrics['silver']:
                    self.logger.info(f"Completude moyenne: {self.metrics['silver']['completeness_pct']:.2f}%")
                if 'anomalies' in self.metrics['silver']:
                    self.logger.info(f"Anomalies: {self.metrics['silver']['anomalies']['total_products_with_anomalies']} produits")
            self.logger.info(f"Metriques: {metrics_file}")
            self.logger.info("="*60)
            
            # Arrêter Spark proprement
            try:
                # Arrêter le SparkContext d'abord
                if hasattr(self, 'spark') and self.spark is not None:
                    self.spark.sparkContext.stop()
                    self.spark.stop()
                    self.logger.info("Session Spark arretee")
            except Exception as e:
                # Ignorer les erreurs de cleanup
                pass
            
            # Forcer le nettoyage de la mémoire (Windows)
            import gc
            gc.collect()


if __name__ == "__main__":
    etl = OpenFoodFactsETL()
    etl.run()