"""
Phase BRONZE - Extraction des données OpenFoodFacts CSV
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)


class BronzeExtractor:
    """Extracteur Bronze - Lecture CSV OpenFoodFacts"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.metrics = {}
    
    def extract_csv(self, source_path: str) -> DataFrame:
        """
        Extrait depuis le CSV OpenFoodFacts
        Format: TSV avec headers
        """
        logger.info(f"=== PHASE BRONZE: Extraction CSV ===")
        logger.info(f"Source: {source_path}")
        
        try:
            # Lecture CSV OpenFoodFacts (séparateur TAB)
            df = self.spark.read.csv(
                source_path,
                header=True,
                sep='\t',
                quote='"',
                escape='\\',
                inferSchema=False,
                multiLine=True
            )
            
            count = df.count()
            self.metrics['products_read'] = count
            self.metrics['source_path'] = source_path
            
            logger.info(f"✓ {count:,} produits extraits")
            
            # Afficher les colonnes disponibles
            logger.info(f"Colonnes disponibles: {len(df.columns)}")
            
            df.cache()
            return df
            
        except Exception as e:
            logger.error(f"✗ Erreur extraction: {e}")
            raise
    
    def extract(self, source_path: str = None) -> DataFrame:
        """Point d'entrée principal"""
        if source_path is None:
            source_path = self.config['openfoodfacts']['raw_data_path']
        
        return self.extract_csv(source_path)
    
    def get_metrics(self) -> dict:
        return self.metrics
