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
        Convertit automatiquement les tableaux (Array) en chaînes (String).
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

        # --- CORRECTION ROBUSTE : Conversion automatique de TOUS les Arrays ---
        # On parcourt le schéma pour trouver toutes les colonnes de type ArrayType
        array_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]
        
        if array_cols:
            print(f"[GOLD] Conversion des colonnes Array -> String : {array_cols}")
            for col_name in array_cols:
                # On transforme ["a", "b"] en "a, b"
                df = df.withColumn(col_name, concat_ws(", ", col(col_name)))
        # ---------------------------------------------------------------------

        print("[GOLD] Demarrage chargement MySQL")

        mysql_conf = self.config['mysql']
        jdbc_url = mysql_conf['jdbc_url']
        
        db_properties = {
            "user": mysql_conf['user'],
            "password": mysql_conf['password'],
            "driver": mysql_conf['driver'],
            "batchsize": "5000",
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