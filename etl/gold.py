"""
Phase GOLD - Chargement DataMart MySQL
- dim_time
- dim_brand
- dim_product (SCD2 réel)
- fact_nutrition_snapshot
"""

import logging
from typing import List

import mysql.connector
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class GoldLoader:
    """Chargeur GOLD vers MySQL avec SCD2 réel (robuste Spark 4)"""

    # ============================================================
    # INIT
    # ============================================================

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config

        mysql = config.get("mysql", {})

        self.jdbc_url = (
            mysql.get("jdbc_url")
            or mysql.get("url")
            or mysql.get("jdbcUrl")
        )

        if not self.jdbc_url:
            raise ValueError("Config MySQL invalide : jdbc_url manquant")

        self.jdbc_props = {
            "user": mysql.get("user"),
            "password": mysql.get("password", ""),
            "driver": mysql.get("driver", "com.mysql.cj.jdbc.Driver"),
            "batchsize": "1000",
        }

        if not self.jdbc_props["user"]:
            raise ValueError("Config MySQL invalide : mysql.user manquant")

        self.mysql_db = mysql.get("database")
        if not self.mysql_db:
            raise ValueError("Config MySQL invalide : mysql.database manquant")

        self.mysql_host = mysql.get("host", "localhost")
        self.mysql_port = int(mysql.get("port", 3306))
        self.mysql_user = mysql["user"]
        self.mysql_password = mysql.get("password", "")

        self._ensure_gold_schema()

    # ============================================================
    # MYSQL UTILS
    # ============================================================

    def _mysql_connect(self):
        return mysql.connector.connect(
            host=self.mysql_host,
            port=self.mysql_port,
            user=self.mysql_user,
            password=self.mysql_password,
            database=self.mysql_db,
        )

    # ============================================================
    # SCHEMA / DDL
    # ============================================================

    def _ensure_gold_schema(self):
        logger.info("[GOLD] Vérification / création schéma")

        conn = self._mysql_connect()
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_time (
            time_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL,
            UNIQUE KEY uq_dim_time_date (date)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_brand (
            brand_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
            brand_name VARCHAR(255) NOT NULL,
            UNIQUE KEY uq_dim_brand_name (brand_name)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_product (
            product_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(64) NOT NULL,
            product_name VARCHAR(512),
            brand_sk BIGINT,
            product_hash CHAR(64),
            effective_from DATETIME NOT NULL,
            effective_to DATETIME NULL,
            is_current TINYINT(1) NOT NULL,
            KEY idx_dim_product_code (code),
            KEY idx_dim_product_current (is_current)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_nutrition_snapshot (
            fact_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
            product_sk BIGINT NOT NULL,
            time_sk BIGINT NOT NULL,
            energy_kcal_100g DOUBLE,
            sugars_100g DOUBLE,
            salt_100g DOUBLE,
            nutriscore_grade VARCHAR(8),
            completeness_score DOUBLE,
            snapshot_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
            KEY idx_fact_product (product_sk),
            KEY idx_fact_time (time_sk)
        )
        """)

        conn.commit()
        cur.close()
        conn.close()

    # ============================================================
    # PUBLIC
    # ============================================================

    def load(self, df: DataFrame):
        logger.info("[GOLD] Démarrage chargement")

        if df is None or df.count() == 0:
            logger.warning("[GOLD] df_silver vide → arrêt")
            return

        self._load_dim_time(df)
        self._load_dim_brand(df)
        self._load_dim_product_scd2(df)
        self._load_fact(df)

    # ============================================================
    # DIM TIME (SAFE SPARK 4)
    # ============================================================

    def _load_dim_time(self, df: DataFrame):
        logger.info("[GOLD] dim_time")

        df_time = (
            df
            .withColumn("ts", F.try_to_timestamp("last_modified_ts"))
            .filter(F.col("ts").isNotNull())
            .select(F.to_date("ts").alias("date"))
            .distinct()
        )

        df_time.write.jdbc(
            self.jdbc_url,
            "dim_time",
            mode="append",
            properties=self.jdbc_props,
        )

    # ============================================================
    # DIM BRAND
    # ============================================================

    def _load_dim_brand(self, df: DataFrame):
        logger.info("[GOLD] dim_brand")

        df_brand = (
            df
            .select(F.col("brands").alias("brand_name"))
            .dropna()
            .distinct()
        )

        df_brand.write.jdbc(
            self.jdbc_url,
            "dim_brand",
            mode="append",
            properties=self.jdbc_props,
        )

    # ============================================================
    # DIM PRODUCT — SCD2
    # ============================================================

    def _load_dim_product_scd2(self, df: DataFrame):
        logger.info("[GOLD] dim_product SCD2")

        df_brand = self.spark.read.jdbc(
            self.jdbc_url, "dim_brand", properties=self.jdbc_props
        )

        df_current = (
            self.spark.read.jdbc(
                self.jdbc_url, "dim_product", properties=self.jdbc_props
            )
            .filter(F.col("is_current") == 1)
            .select(
                F.col("code").alias("tgt_code"),
                F.col("product_hash").alias("tgt_hash"),
            )
        )

        df_src = (
            df.join(df_brand, df.brands == df_brand.brand_name, "left")
            .select(
                "code",
                "product_name",
                "brand_sk",
                "product_hash",
            )
        )

        df_join = df_src.join(
            df_current, df_src.code == df_current.tgt_code, "left"
        )

        df_new = df_join.filter(F.col("tgt_code").isNull())

        df_changed = df_join.filter(
            (F.col("tgt_code").isNotNull()) &
            (F.col("product_hash") != F.col("tgt_hash"))
        )

        changed_codes = [
            r.code for r in df_changed.select("code").distinct().collect()
        ]

        if changed_codes:
            self._close_versions(changed_codes)

        df_insert = (
            df_new.select("code", "product_name", "brand_sk", "product_hash")
            .unionByName(
                df_changed.select("code", "product_name", "brand_sk", "product_hash")
            )
            .withColumn("effective_from", F.current_timestamp())
            .withColumn("effective_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(1))
        )

        df_insert.write.jdbc(
            self.jdbc_url,
            "dim_product",
            mode="append",
            properties=self.jdbc_props,
        )

    def _close_versions(self, codes: List[str]):
        logger.info("[GOLD] Fermeture SCD2 (%d produits)", len(codes))

        conn = self._mysql_connect()
        cur = conn.cursor()

        placeholders = ",".join(["%s"] * len(codes))
        sql = f"""
        UPDATE dim_product
        SET effective_to = NOW(), is_current = 0
        WHERE is_current = 1 AND code IN ({placeholders})
        """

        cur.execute(sql, tuple(codes))
        conn.commit()
        cur.close()
        conn.close()

    # ============================================================
    # FACT (SAFE SPARK 4)
    # ============================================================

    def _load_fact(self, df: DataFrame):
        logger.info("[GOLD] fact_nutrition_snapshot")

        df_prod = (
            self.spark.read.jdbc(
                self.jdbc_url, "dim_product", properties=self.jdbc_props
            )
            .filter(F.col("is_current") == 1)
            .select("product_sk", "code")
        )

        df_time = self.spark.read.jdbc(
            self.jdbc_url, "dim_time", properties=self.jdbc_props
        )

        df_fact = (
            df
            .withColumn("ts", F.try_to_timestamp("last_modified_ts"))
            .filter(F.col("ts").isNotNull())
            .join(df_prod, "code")
            .join(df_time, F.to_date("ts") == df_time.date)
            .select(
                "product_sk",
                "time_sk",
                "energy_kcal_100g",
                "sugars_100g",
                "salt_100g",
                "nutriscore_grade",
                "completeness_score",
            )
        )

        df_fact.write.jdbc(
            self.jdbc_url,
            "fact_nutrition_snapshot",
            mode="append",
            properties=self.jdbc_props,
        )
