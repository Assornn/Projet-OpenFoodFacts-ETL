"""
Phase GOLD - Chargement MySQL (Dims + Fact) + SCD2 réel sur dim_product

- dim_time: basé sur last_modified_t -> date + year/month/week
- dim_brand: liste distincte des marques
- dim_product: SCD2 réel (close + insert) sur (code) en comparant les attributs suivis
- fact_nutrition_snapshot: FK vers product_sk (current) et time_sk
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, split, element_at,
    to_timestamp, to_date, from_unixtime, year, month, weekofyear,
    lit, current_timestamp, sha2, concat_ws, when
)
from pyspark.sql.types import LongType, StringType

logger = logging.getLogger(__name__)


class GoldLoader:
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.metrics: Dict = {"gold": {}}

        mysql_cfg = config.get("mysql", {})
        self.url = mysql_cfg["url"]
        self.user = mysql_cfg["user"]
        self.password = mysql_cfg["password"]
        self.driver = mysql_cfg.get("driver", "com.mysql.cj.jdbc.Driver")
        self.batchsize = int(mysql_cfg.get("batchsize", 2000))

    # -------------------------
    # JDBC helpers
    # -------------------------
    def _jdbc_props(self) -> Dict[str, str]:
        return {
            "user": self.user,
            "password": self.password,
            "driver": self.driver
        }

    def _exec_sql(self, sql: str) -> None:
        """
        Exécute du SQL côté MySQL via JDBC, sans dépendre d'un connecteur python.
        """
        jvm = self.spark._sc._gateway.jvm
        conn = None
        stmt = None
        try:
            conn = jvm.java.sql.DriverManager.getConnection(self.url, self.user, self.password)
            stmt = conn.createStatement()
            stmt.execute(sql)
        finally:
            try:
                if stmt is not None:
                    stmt.close()
            except Exception:
                pass
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    def _read_table(self, table: str) -> DataFrame:
        return self.spark.read.jdbc(self.url, table, properties=self._jdbc_props())

    # -------------------------
    # Main entrypoint
    # -------------------------
    def load(self, df_silver: DataFrame) -> None:
        logger.info("Chargement GOLD vers MySQL")

        # 1) dims (time, brand)
        self._load_dim_time(df_silver)
        self._load_dim_brand(df_silver)

        # 2) dim_product SCD2 réel
        self._load_dim_product_scd2(df_silver)

        # 3) fact
        self._load_fact_nutrition_snapshot(df_silver)

    def get_metrics(self) -> dict:
        return self.metrics

    # -------------------------
    # DIM TIME
    # -------------------------
    def _silver_to_date(self, df: DataFrame) -> DataFrame:
        """
        Construit une colonne `snap_date` (DateType) à partir de `last_modified_t`.
        last_modified_t: Unix seconds (num) ou string num.
        """
        # cast safe -> long
        df = df.withColumn("last_modified_t_long", col("last_modified_t").cast(LongType()))

        # Si long ok -> from_unixtime -> timestamp -> date
        # sinon, on essaie timestamp direct (au cas où)
        df = df.withColumn(
            "snap_date",
            when(
                col("last_modified_t_long").isNotNull(),
                to_date(from_unixtime(col("last_modified_t_long")))
            ).otherwise(
                to_date(to_timestamp(col("last_modified_t")))
            )
        )
        return df

    def _load_dim_time(self, df_silver: DataFrame) -> None:
        logger.info("[GOLD] dim_time")

        df = self._silver_to_date(df_silver)

        df_time = (
            df.select(col("snap_date").alias("date"))
              .where(col("date").isNotNull())
              .distinct()
              .withColumn("year", year(col("date")))
              .withColumn("month", month(col("date")))
              .withColumn("week", weekofyear(col("date")))
        )

        # Upsert simple (petite dimension) : on évite DROP/TRUNCATE (FK).
        # On insert seulement les dates manquantes.
        try:
            existing = self._read_table("dim_time").select("date").distinct()
            df_time = df_time.join(existing, on="date", how="left_anti")
        except Exception:
            # si table vide ou lecture échoue, on insère tout
            pass

        count = df_time.count()
        if count == 0:
            logger.info("✓ dim_time chargée : 0 lignes (aucune nouvelle date)")
            self.metrics["gold"]["dim_time_inserted"] = 0
            return

        df_time.write.jdbc(
            url=self.url,
            table="dim_time",
            mode="append",
            properties=self._jdbc_props()
        )

        logger.info(f"✓ dim_time chargée : {count} lignes")
        self.metrics["gold"]["dim_time_inserted"] = count

    # -------------------------
    # DIM BRAND
    # -------------------------
    def _normalize_brand(self, df: DataFrame) -> DataFrame:
        """
        Prend la 1ère marque (brands peut contenir "A, B, C").
        Normalise: trim, lower, nettoyage espaces multiples.
        """
        # brands -> 1ère valeur
        df = df.withColumn("brand_raw", element_at(split(col("brands"), ","), 1))
        df = df.withColumn("brand_name",
                           regexp_replace(lower(trim(col("brand_raw"))), r"\s+", " "))
        return df

    def _load_dim_brand(self, df_silver: DataFrame) -> None:
        logger.info("[GOLD] dim_brand")

        df = self._normalize_brand(df_silver)

        df_brand = (
            df.select(col("brand_name"))
              .where(col("brand_name").isNotNull() & (col("brand_name") != lit("")))
              .distinct()
        )

        # Insert only missing brands
        try:
            existing = self._read_table("dim_brand").select("brand_name").distinct()
            df_brand = df_brand.join(existing, on="brand_name", how="left_anti")
        except Exception:
            pass

        count = df_brand.count()
        if count == 0:
            logger.info("✓ dim_brand chargée : 0 lignes (aucune nouvelle marque)")
            self.metrics["gold"]["dim_brand_inserted"] = 0
            return

        df_brand.write.jdbc(
            url=self.url,
            table="dim_brand",
            mode="append",
            properties=self._jdbc_props()
        )

        logger.info(f"✓ dim_brand chargée : {count} lignes")
        self.metrics["gold"]["dim_brand_inserted"] = count

    # -------------------------
    # DIM PRODUCT (SCD2 réel)
    # -------------------------
    def _load_dim_product_scd2(self, df_silver: DataFrame) -> None:
        """
        SCD2 réel sur dim_product (code = clé naturelle).
        Attributs suivis (dans ta table actuelle) : product_name + brand_sk
        """
        logger.info("[GOLD] dim_product (SCD2)")

        # Ref brand (brand_name -> brand_sk)
        df_brand_ref = self._read_table("dim_brand").select("brand_sk", "brand_name")

        # Préparer flux entrant
        df_in = self._normalize_brand(df_silver).select(
            trim(col("code")).alias("code"),
            col("product_name"),
            col("brand_name")
        )

        df_in = (
            df_in.join(df_brand_ref, on="brand_name", how="left")
                .drop("brand_name")
        )

        # Hash d'attributs suivis (pour détecter changements)
        df_in = df_in.withColumn(
            "attr_hash",
            sha2(concat_ws("||",
                           col("product_name").cast(StringType()),
                           col("brand_sk").cast(StringType())), 256)
        )

        # Charger existants (versions courantes)
        df_cur = self._read_table("dim_product").where(col("is_current") == lit(1)).select(
            "product_sk", "code", "product_name", "brand_sk", "effective_from", "effective_to", "is_current"
        )

        df_cur = df_cur.withColumn(
            "attr_hash",
            sha2(concat_ws("||",
                           col("product_name").cast(StringType()),
                           col("brand_sk").cast(StringType())), 256)
        )

        # Join pour identifier NEW / CHANGED / UNCHANGED
        df_join = df_in.join(df_cur.select("code", "attr_hash"), on="code", how="left") \
                       .withColumnRenamed("attr_hash", "in_hash") \
                       .withColumnRenamed("attr_hash", "cur_hash")

        # Spark rename propre (sinon collision) :
        df_join = df_in.alias("i").join(
            df_cur.select(col("code").alias("code"), col("attr_hash").alias("cur_hash")).alias("c"),
            on="code",
            how="left"
        ).withColumnRenamed("attr_hash", "in_hash")

        df_new = df_join.filter(col("cur_hash").isNull())
        df_changed = df_join.filter(col("cur_hash").isNotNull() & (col("in_hash") != col("cur_hash")))
        df_unchanged = df_join.filter(col("cur_hash").isNotNull() & (col("in_hash") == col("cur_hash")))

        new_count = df_new.count()
        changed_count = df_changed.count()
        unchanged_count = df_unchanged.count()

        # 1) Fermer les anciennes lignes (CHANGED)
        if changed_count > 0:
            # staging table de codes à fermer
            stage_tbl = "etl_stage_changed_codes"
            df_changed_codes = df_changed.select("code").distinct()

            df_changed_codes.write.jdbc(
                url=self.url,
                table=stage_tbl,
                mode="overwrite",
                properties=self._jdbc_props()
            )

            self._exec_sql(f"""
                UPDATE dim_product p
                JOIN {stage_tbl} s ON p.code = s.code
                SET p.is_current = 0,
                    p.effective_to = NOW()
                WHERE p.is_current = 1;
            """)

            # Cleanup (optionnel)
            try:
                self._exec_sql(f"DROP TABLE IF EXISTS {stage_tbl};")
            except Exception:
                pass

        # 2) Insérer nouvelles versions (NEW + CHANGED)
        df_to_insert = (
            df_new.select("code", "product_name", "brand_sk")
            .unionByName(df_changed.select("code", "product_name", "brand_sk"))
            .withColumn("effective_from", current_timestamp())
            .withColumn("effective_to", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(1))
        )

        insert_count = df_to_insert.count()
        if insert_count > 0:
            df_to_insert.write.jdbc(
                url=self.url,
                table="dim_product",
                mode="append",
                properties=self._jdbc_props()
            )

        logger.info(
            f"✓ dim_product SCD2: new={new_count}, changed={changed_count}, unchanged={unchanged_count}, inserted={insert_count}"
        )
        self.metrics["gold"]["dim_product_scd2"] = {
            "new": new_count,
            "changed": changed_count,
            "unchanged": unchanged_count,
            "inserted": insert_count
        }

    # -------------------------
    # FACT
    # -------------------------
    def _load_fact_nutrition_snapshot(self, df_silver: DataFrame) -> None:
        logger.info("[GOLD] fact_nutrition_snapshot")

        # Refs
        df_time_ref = self._read_table("dim_time").select("time_sk", "date")
        df_product_ref = self._read_table("dim_product").where(col("is_current") == lit(1)).select("product_sk", "code")

        # Date snapshot
        df = self._silver_to_date(df_silver)

        # Join SK product + time
        df_fact = (
            df.join(df_product_ref, on="code", how="left")
              .join(df_time_ref, df_time_ref["date"] == df["snap_date"], how="left")
        )

        # Colonnes fact (reste flexible si certaines manquent)
        def safe_col(name: str):
            return col(name) if name in df_fact.columns else lit(None)

        df_fact = df_fact.select(
            col("product_sk"),
            col("time_sk"),
            safe_col("energy_kcal_100g").cast("decimal(10,2)").alias("energy_kcal_100g"),
            safe_col("sugars_100g").cast("decimal(10,2)").alias("sugars_100g"),
            safe_col("salt_100g").cast("decimal(10,2)").alias("salt_100g"),
            safe_col("nutriscore_grade").cast("string").alias("nutriscore_grade"),
            safe_col("completeness_score").cast("decimal(5,4)").alias("completeness_score")
        )

        # Idempotence simple sur fact snapshot : on recharge à blanc
        # (tu peux remplacer par UPSERT si tu ajoutes une clé unique)
        self._exec_sql("TRUNCATE TABLE fact_nutrition_snapshot;")

        df_fact.write.jdbc(
            url=self.url,
            table="fact_nutrition_snapshot",
            mode="append",
            properties=self._jdbc_props()
        )

        count = df_fact.count()
        logger.info(f"✓ fact_nutrition_snapshot chargée : {count} lignes")
        self.metrics["gold"]["fact_nutrition_snapshot_inserted"] = count
