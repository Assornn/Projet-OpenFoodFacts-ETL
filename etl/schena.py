from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)

OPENFOODFACTS_SCHEMA = StructType([

    StructField("code", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("brands", StringType(), True),
    StructField("categories", StringType(), True),
    StructField("countries", StringType(), True),

    StructField("energy_kcal_100g", DoubleType(), True),
    StructField("fat_100g", DoubleType(), True),
    StructField("sugars_100g", DoubleType(), True),
    StructField("proteins_100g", DoubleType(), True),
    StructField("salt_100g", DoubleType(), True),

    StructField("nutriscore_grade", StringType(), True),
    StructField("last_modified_datetime", StringType(), True)
])
