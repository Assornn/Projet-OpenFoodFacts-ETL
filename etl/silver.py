"""
Phase SILVER - Transformation et nettoyage
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)


class SilverTransformer:
    """Transformateur Silver"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.metrics = {}
    
    def clean_and_normalize(self, df: DataFrame) -> DataFrame:
        """Nettoyage de base et normalisation des champs"""
        logger.info("=== PHASE SILVER: Nettoyage et normalisation ===")
        
        # Sélection et nettoyage des champs principaux
        df = df.select(
            # Code-barres (clé naturelle)
            trim(col("code")).alias("code"),
            
            # Nom du produit (priorité langue)
            coalesce(
                trim(col("product_name_fr")),
                trim(col("product_name_en")),
                trim(col("product_name"))
            ).alias("product_name"),
            
            # Autres champs texte
            trim(col("brands")).alias("brands"),
            trim(col("categories")).alias("categories"),
            trim(col("countries")).alias("countries"),
            
            # Timestamps et scores
            col("last_modified_t"),
            lower(trim(col("nutriscore_grade"))).alias("nutriscore_grade"),
            col("nova_group"),
            lower(trim(col("ecoscore_grade"))).alias("ecoscore_grade")
        )
        
        logger.info("OK Nettoyage des champs effectue")
        return df
    
    def filter_invalid_records(self, df: DataFrame) -> DataFrame:
        """Filtre les enregistrements invalides"""
        logger.info("Filtrage des enregistrements invalides...")
        
        count_before = df.count()
        
        # Filtres de qualité minimale
        df = df.filter(
            (col("code").isNotNull()) & 
            (length(col("code")) >= 8) &
            (col("code").rlike("^[0-9]+$"))
        )
        
        count_after = df.count()
        rejected = count_before - count_after
        
        self.metrics['products_input'] = count_before
        self.metrics['products_filtered'] = count_after
        self.metrics['products_rejected'] = rejected
        # Utiliser le format Python, pas PySpark round
        if count_before > 0:
            self.metrics['rejection_rate_pct'] = float(f"{(rejected / count_before * 100):.2f}")
        else:
            self.metrics['rejection_rate_pct'] = 0.0
        
        logger.info(f"OK Filtrage: {count_after:,} produits conserves ({rejected:,} rejetes)")
        
        return df
    
    def extract_nutrients(self, df: DataFrame) -> DataFrame:
        """Extrait et normalise les valeurs nutritionnelles"""
        logger.info("Extraction des nutriments...")
        
        # Obtenir les colonnes disponibles
        available_cols = df.columns
        
        # Mapping des nutriments
        nutrients = {
            'energy_kcal_100g': ['energy-kcal_100g', 'energy_kcal_100g'],
            'energy_kj_100g': ['energy-kj_100g', 'energy_kj_100g'],
            'fat_100g': ['fat_100g'],
            'saturated_fat_100g': ['saturated-fat_100g', 'saturated_fat_100g'],
            'carbohydrates_100g': ['carbohydrates_100g'],
            'sugars_100g': ['sugars_100g'],
            'fiber_100g': ['fiber_100g'],
            'proteins_100g': ['proteins_100g'],
            'salt_100g': ['salt_100g'],
            'sodium_100g': ['sodium_100g']
        }
        
        # Extraire chaque nutriment
        for target_col, possible_names in nutrients.items():
            found = False
            for name in possible_names:
                if name in available_cols:
                    df = df.withColumn(target_col, col(name).cast(DoubleType()))
                    found = True
                    break
            
            if not found:
                df = df.withColumn(target_col, lit(None).cast(DoubleType()))
        
        logger.info("OK Nutriments extraits")
        return df
    
    def harmonize_salt_sodium(self, df: DataFrame) -> DataFrame:
        """Harmonise les valeurs sel/sodium"""
        logger.info("Harmonisation sel/sodium...")
        
        df = df.withColumn("salt_100g",
            when(
                col("salt_100g").isNull() & col("sodium_100g").isNotNull(),
                col("sodium_100g") * lit(2.5)
            ).otherwise(col("salt_100g"))
        )
        
        df = df.withColumn("sodium_100g",
            when(
                col("sodium_100g").isNull() & col("salt_100g").isNotNull(),
                col("salt_100g") / lit(2.5)
            ).otherwise(col("sodium_100g"))
        )
        
        logger.info("OK Harmonisation sel/sodium effectuee")
        return df
    
    def deduplicate(self, df: DataFrame) -> DataFrame:
        """Dédoublonnage par code-barres"""
        logger.info("Dedoublonnage par code-barres...")
        
        count_before = df.count()
        
        window = Window.partitionBy("code").orderBy(desc("last_modified_t"))
        df = df.withColumn("row_num", row_number().over(window))
        df = df.filter(col("row_num") == 1).drop("row_num")
        
        count_after = df.count()
        duplicates = count_before - count_after
        
        self.metrics['duplicates_removed'] = duplicates
        
        logger.info(f"OK Dedoublonnage: {duplicates:,} doublons supprimes")
        
        return df
    
    def calculate_completeness(self, df: DataFrame) -> DataFrame:
        """Calcule le score de complétude"""
        logger.info("Calcul des scores de completude...")
        
        df = df.withColumn("completeness_score",
            (
                when(col("product_name").isNotNull(), lit(1)).otherwise(lit(0)) +
                when(col("brands").isNotNull(), lit(1)).otherwise(lit(0)) +
                when(col("energy_kcal_100g").isNotNull(), lit(1)).otherwise(lit(0)) +
                when(col("fat_100g").isNotNull(), lit(1)).otherwise(lit(0)) +
                when(col("sugars_100g").isNotNull(), lit(1)).otherwise(lit(0))
            ) / lit(5.0)
        )
        
        logger.info("OK Completude calculee")
        return df
    
    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        """Détecte les anomalies"""
        logger.info("Detection des anomalies...")
        
        df = df.withColumn("quality_issues", 
            array(
                when(col("sugars_100g") > lit(100), lit("sugars_out_of_bounds")),
                when(col("salt_100g") > lit(25), lit("salt_out_of_bounds")),
                when(col("fat_100g") > lit(100), lit("fat_out_of_bounds")),
                when(col("proteins_100g") > lit(100), lit("proteins_out_of_bounds")),
                when(col("energy_kcal_100g") > lit(900), lit("energy_out_of_bounds")),
                when(col("energy_kcal_100g") < lit(0), lit("negative_energy")),
                when(col("sugars_100g") < lit(0), lit("negative_sugars")),
                when(col("salt_100g") < lit(0), lit("negative_salt"))
            )
        )
        
        df = df.withColumn("quality_issues", 
            expr("filter(quality_issues, x -> x is not null)")
        )
        
        anomalies_count = df.filter(size(col("quality_issues")) > 0).count()
        
        logger.info(f"OK Anomalies detectees: {anomalies_count:,} produits")
        
        return df
    
    def transform(self, df_bronze: DataFrame) -> DataFrame:
        """Pipeline complet de transformation Silver"""
        logger.info("\n" + "="*60)
        logger.info("=== DEMARRAGE PHASE SILVER ===")
        logger.info("="*60 + "\n")
        
        count_initial = df_bronze.count()
        
        df = df_bronze
        df = self.clean_and_normalize(df)
        df = self.filter_invalid_records(df)
        df = self.extract_nutrients(df)
        df = self.harmonize_salt_sodium(df)
        df = self.deduplicate(df)
        df = self.calculate_completeness(df)
        df = self.detect_anomalies(df)
        
        # Calcul des métriques finales
        count_final = df.count()
        avg_compl = df.agg(avg("completeness_score")).first()[0]
        
        # Utiliser format Python
        self.metrics['avg_completeness'] = float(f"{avg_compl:.4f}") if avg_compl else 0.0
        self.metrics['completeness_pct'] = float(f"{(avg_compl * 100):.2f}") if avg_compl else 0.0
        
        # Anomalies
        anomalies_total = df.filter(size(col("quality_issues")) > 0).count()
        self.metrics['anomalies'] = {
            'total_products_with_anomalies': anomalies_total,
            'anomaly_rate_pct': float(f"{(anomalies_total / count_final * 100):.2f}") if count_final > 0 else 0.0,
            'by_rule': {}
        }
        
        # Compter par type
        if count_final > 0:
            anomaly_types = [
                'sugars_out_of_bounds',
                'salt_out_of_bounds', 
                'fat_out_of_bounds',
                'proteins_out_of_bounds',
                'energy_out_of_bounds',
                'negative_energy',
                'negative_sugars',
                'negative_salt'
            ]
            for anomaly in anomaly_types:
                count = df.filter(array_contains(col("quality_issues"), anomaly)).count()
                if count > 0:
                    self.metrics['anomalies']['by_rule'][anomaly] = count
        
        df.cache()
        
        logger.info("\n" + "="*60)
        logger.info("=== PHASE SILVER TERMINEE ===")
        logger.info(f"Produits finaux: {count_final:,}")
        logger.info(f"Completude moyenne: {self.metrics['completeness_pct']:.2f}%")
        logger.info(f"Anomalies: {anomalies_total} ({self.metrics['anomalies']['anomaly_rate_pct']:.2f}%)")
        logger.info("="*60 + "\n")
        
        return df
    
    def get_metrics(self) -> dict:
        return self.metrics