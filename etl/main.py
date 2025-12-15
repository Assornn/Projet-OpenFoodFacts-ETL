"""
ETL OpenFoodFacts - Orchestrateur Principal
"""

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
    
    def __init__(self, config_path='config.yaml'):
        self._setup_logging()
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        self.metrics = {
            'run_timestamp': datetime.now().isoformat(),
            'status': 'started'
        }
    
    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/etl.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _load_config(self, config_path):
        # Priorité config.local.yaml
        local = Path('config.local.yaml')
        if local.exists():
            config_path = 'config.local.yaml'
        
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _create_spark_session(self):
        builder = SparkSession.builder \
            .appName(self.config['spark']['app_name']) \
            .master(self.config['spark']['master']) \
            .config("spark.driver.memory", self.config['spark']['driver_memory'])
        
        mysql_jar = self.config['spark'].get('mysql_jar')
        if mysql_jar and Path(mysql_jar).exists():
            builder = builder.config("spark.jars", mysql_jar)
        
        return builder.getOrCreate()
    
    def run(self):
        """Exécute le pipeline complet"""
        self.logger.info("\n" + "="*60)
        self.logger.info("🚀 DÉMARRAGE ETL OPENFOODFACTS")
        self.logger.info("="*60)
        
        start = datetime.now()
        
        try:
            # Bronze
            extractor = BronzeExtractor(self.spark, self.config)
            df_bronze = extractor.extract()
            self.metrics['bronze'] = extractor.get_metrics()
            
            # Silver
            transformer = SilverTransformer(self.spark, self.config)
            df_silver = transformer.transform(df_bronze)
            self.metrics['silver'] = transformer.get_metrics()
            
            # Gold
            loader = GoldLoader(self.spark, self.config)
            loader.load(df_silver)
            self.metrics['gold'] = loader.get_metrics()
            
            self.metrics['status'] = 'success'
            
        except Exception as e:
            self.logger.error(f"✗ Erreur: {e}")
            self.metrics['status'] = 'failed'
            self.metrics['error'] = str(e)
            raise
        
        finally:
            duration = (datetime.now() - start).total_seconds()
            self.metrics['duration_seconds'] = duration
            
            # Sauvegarder métriques
            output_dir = Path('output/metrics')
            output_dir.mkdir(parents=True, exist_ok=True)
            
            metrics_file = output_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(metrics_file, 'w') as f:
                json.dump(self.metrics, f, indent=2)
            
            self.logger.info(f"\n✓ Durée: {duration:.2f}s")
            self.logger.info(f"✓ Statut: {self.metrics['status']}")
            
            self.spark.stop()


if __name__ == "__main__":
    etl = OpenFoodFactsETL()
    etl.run()
