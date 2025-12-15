"""
Phase GOLD - Chargement DataMart MySQL
SCD2 réel sur dim_product
"""

import logging
import mysql.connector
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    to_date,
    sha2,
    concat_ws
)

logger = logging.getLogger(__name__)


class GoldLoader:
    """Chargeur GOLD vers MySQL avec SCD2"""

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config

        mysql_cfg = config.get("mysql", {})

        self.jdbc_url = mysql_cfg.get("jdbc_url")
        if not self.jdbc_url or not mysql_cfg.get("user"):
            raise ValueError(
                "Config MySQL invalide: mysql.jdbc_url / mysql.user manquants"
            )

        self.jdbc_props = {
            "user": mysql_cfg["user"],
            "password": mysql_cfg.get("password", ""),
            "driver": "com.mysql.cj.jdbc.Driver",
            "batchsize": "1000"
        }

    # ============================================================
    # PUBLIC
    # ============================================================

    def load(self, df_silver: DataFrame):
        logger.info("Chargement GOLD vers MySQL")

        self._load_dim_time(df_silver)
        self._load_dim_brand(df_silver)
        self._load_dim_product_scd2(df_silver)
        self._load_fact_nutrition(df_silver)

    # ============================================================
    # DIM TIME
    # ============================================================

    def _load_dim_time(self, df: DataFrame):
        logger.info("[GOLD] dim_time")

        df_time = (
            df.select(to_date(col("last_modified_ts")).alias("date"))
              .dropna()
              .distinct()
        )

        df_time.write.jdbc(
            url=self.jdbc_url,
            table="dim_time",
            mode="append",
            properties=self.jdbc_props
        )

    # ============================================================
    # DIM BRAND
    # ============================================================

    def _load_dim_brand(self, df: DataFrame):
        logger.info("[GOLD] dim_brand")

        df_brand = (
            df.select(col("brands").alias("brand_name"))
              .dropna()
              .distinct()
        )

        df_brand.write.jdbc(
            url=self.jdbc_url,
            table="dim_brand",
            mode="append",
            properties=self.jdbc_props
        )

    # ============================================================
    # DIM PRODUCT — SCD2 RÉEL
    # ============================================================

    def _load_dim_product_scd2(self, df: DataFrame):
        logger.info("[GOLD] dim_product (SCD2 réel)")

        # =========================
        # Source avec hash métier
        # =========================
        df_src = (
            df.withColumn(
                "product_hash",
                sha2(
                    concat_ws(
                        "||",
                        col("product_name"),
                        col("brands"),
                        col("categories"),
                        col("countries"),
                        col("nutriscore_grade")
                    ),
                    256
                )
            )
            .select(
                col("code").alias("src_code"),
                col("product_name").alias("src_product_name"),
                col("product_hash").alias("src_product_hash")
            )
        )

        # =========================
        # Dimension actuelle
        # =========================
        df_tgt = (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="dim_product",
                properties=self.jdbc_props
            )
            .filter(col("is_current") == 1)
            .select(
                col("code").alias("tgt_code"),
                col("product_sk"),
                col("product_name").alias("tgt_product_name")
            )
        )

        # =========================
        # Jointure
        # =========================
        df_join = df_src.join(
            df_tgt,
            df_src.src_code == df_tgt.tgt_code,
            "left"
        )

        # =========================
        # Nouveaux produits
        # =========================
        df_new = df_join.filter(col("tgt_code").isNull())

        # =========================
        # Produits modifiés (SCD2)
        # =========================
        df_changed = df_join.filter(
            col("tgt_code").isNotNull()
            & (col("src_product_name") != col("tgt_product_name"))
        )

        # =========================
        # Fermeture anciennes lignes
        # =========================
        if df_changed.count() > 0:
            self._close_previous_versions(df_changed)

        # =========================
        # Insertion nouvelles lignes
        # =========================
        df_new_insert = df_new.select(
            col("src_code").alias("code"),
            col("src_product_name").alias("product_name"),
            current_timestamp().alias("effective_from"),
            lit(None).cast("timestamp").alias("effective_to"),
            lit(1).alias("is_current")
        )

        df_changed_insert = df_changed.select(
            col("src_code").alias("code"),
            col("src_product_name").alias("product_name"),
            current_timestamp().alias("effective_from"),
            lit(None).cast("timestamp").alias("effective_to"),
            lit(1).alias("is_current")
        )

        df_insert = df_new_insert.unionByName(df_changed_insert)

        df_insert.write.jdbc(
            url=self.jdbc_url,
            table="dim_product",
            mode="append",
            properties=self.jdbc_props
        )

    # ============================================================
    # FERMETURE SCD2
    # ============================================================

    def _close_previous_versions(self, df_changed: DataFrame):
        logger.info("Fermeture des anciennes versions SCD2")

        mysql_cfg = self.config["mysql"]

        conn = mysql.connector.connect(
            host=mysql_cfg["host"],
            port=mysql_cfg["port"],
            user=mysql_cfg["user"],
            password=mysql_cfg["password"],
            database=mysql_cfg["database"]
        )
        cursor = conn.cursor()

        codes = [
            r["src_code"]
            for r in df_changed.select("src_code").distinct().collect()
        ]

        for code in codes:
            cursor.execute(
                """
                UPDATE dim_product
                SET effective_to = NOW(),
                    is_current = 0
                WHERE code = %s AND is_current = 1
                """,
                (code,)
            )

        conn.commit()
        cursor.close()
        conn.close()

    # ============================================================
    # FACT NUTRITION
    # ============================================================

    def _load_fact_nutrition(self, df: DataFrame):
        logger.info("[GOLD] fact_nutrition_snapshot")

        df_dim_product = (
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table="dim_product",
                properties=self.jdbc_props
            )
            .filter(col("is_current") == 1)
        )

        df_dim_time = self.spark.read.jdbc(
            url=self.jdbc_url,
            table="dim_time",
            properties=self.jdbc_props
        )

        df_fact = (
            df.join(df_dim_product, "code")
              .join(df_dim_time, to_date(df.last_modified_ts) == df_dim_time.date)
              .select(
                  col("product_sk"),
                  col("time_sk"),
                  col("energy_kcal_100g"),
                  col("sugars_100g"),
                  col("salt_100g"),
                  col("nutriscore_grade"),
                  col("completeness_score")
              )
        )

        df_fact.write.jdbc(
            url=self.jdbc_url,
            table="fact_nutrition_snapshot",
            mode="append",
            properties=self.jdbc_props
        )
