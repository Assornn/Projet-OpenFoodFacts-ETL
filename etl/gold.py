from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType
from pyspark.sql.functions import col, concat_ws

class GoldLoader:
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config

    def load(self, df):
        """
        Charge les données agrégées/nettoyées dans MySQL (DataMart).
        """
        print("[GOLD] Verification / creation schema")
        
        if df is None:
            print("[WARN] [GOLD] DataFrame None, pas de chargement.")
            return

        try:
            if len(df.head(1)) == 0:
                print("[WARN] [GOLD] DataFrame vide, pas de chargement.")
                return
        except Exception:
            return

        # 1. Conversion des Arrays en String
        array_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]
        if array_cols:
            print(f"[GOLD] Conversion des colonnes Array -> String : {array_cols}")
            for col_name in array_cols:
                df = df.withColumn(col_name, concat_ws(", ", col(col_name)))

        # 2. OPTIMISATION CRITIQUE : REPARTITION
        # On réduit le nombre de partitions à 8 pour éviter d'ouvrir 200 connexions MySQL
        # et de saturer la mémoire RAM (Heap Space).
        print(f"[GOLD] Optimisation : Reduction de {df.rdd.getNumPartitions()} partitions a 8 partitions.")
        df = df.repartition(8)

        print("[GOLD] Demarrage chargement MySQL")

        mysql_conf = self.config['mysql']
        jdbc_url = mysql_conf['jdbc_url']
        
        # On garde un batchsize raisonnable
        db_properties = {
            "user": mysql_conf['user'],
            "password": mysql_conf['password'],
            "driver": mysql_conf['driver'],
            "batchsize": "2000", # Réduit légèrement pour soulager la RAM (5000 -> 2000)
            "rewriteBatchedStatements": "true"
        }

        try:
            table_name = "gold_products"

            df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode="overwrite", 
                properties=db_properties
            )
            
            print(f"[SUCCESS] [GOLD] Chargement termine dans la table '{table_name}'")

        except Exception as e:
            print(f"[ERROR] [GOLD] Erreur ecriture MySQL : {e}")
            raise e