from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

class BronzeExtractor:
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.limit_applied = None # Pour les métriques

    def extract(self):
        """
        Point d'entrée principal de l'extraction Bronze.
        """
        print("=== PHASE BRONZE: Extraction OpenFoodFacts ===")
        
        # Récupération du chemin du fichier raw depuis la config
        raw_path = self.config['openfoodfacts']['raw_data_path']
        print(f"Source: {raw_path}")

        return self.extract_csv(raw_path)

    def extract_csv(self, source_path):
        """
        Lit le fichier CSV (TSV compressé) et sélectionne les colonnes pertinentes.
        Gère le Schema Drift (changement de nom de colonnes).
        """
        
        # 1. Lecture brute du CSV avec les paramètres de la config
        df_raw = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("delimiter", "\t") \
            .option("inferSchema", "false") \
            .load(source_path)

        # 2. Sélection et Cast des colonnes
        try:
            df = df_raw.select(
                col("code").cast("string"),
                col("product_name").cast("string"),
                col("brands").cast("string"),
                col("categories").cast("string"),
                col("countries").cast("string"),
                
                # Nutriments
                col("energy-kcal_100g").cast("float"),
                col("fat_100g").cast("float"),
                col("sugars_100g").cast("float"),
                col("proteins_100g").cast("float"),
                col("salt_100g").cast("float"),
                
                col("nutriscore_grade").cast("string"),
                
                # Correction Schema Drift
                col("completeness").alias("completeness_score").cast("float"),

                col("last_modified_t").cast("long")
            )

            # Ajout de métadonnées
            df = df.withColumn("ingestion_date", current_timestamp())

            # 3. Gestion sécurisée de la limite
            bronze_conf = self.config.get('bronze') or {}
            limit_rows = bronze_conf.get('limit_rows')

            if limit_rows:
                print(f"[INFO] LIMIT APPLIQUEE: {limit_rows} lignes")
                df = df.limit(limit_rows)
                self.limit_applied = limit_rows

            return df

        except Exception as e:
            print(f"[ERREUR] Erreur lors de la selection des colonnes dans Bronze : {e}")
            raise e

    def get_metrics(self):
        """
        Retourne les métriques de l'extraction pour le rapport final.
        C'est cette fonction qui manquait !
        """
        return {
            "source_type": "csv",
            "limit_applied": self.limit_applied if self.limit_applied else "Aucune"
        }