# Databricks notebook source
# DBTITLE 1,Set Parameters
# MAGIC %md # Silver Layer
# MAGIC Notebook to clean raw (bronze) data to silver

# COMMAND ----------

# DBTITLE 1,Set Parameters
dbutils.widgets.text("env", "")

# COMMAND ----------

# DBTITLE 1,Get parameters
env = dbutils.widgets.get("env")

# COMMAND ----------

# DBTITLE 1,Define Config
path_volume = "/Volumes/dev_bronze/r_source_h/source_general"
layer = "silver"
source_system = "source"
entity = "turbine_measurement"
table = f"{env}_{layer}.b_{source_system}_h.{entity}"

# Define Paths
# bronze_path = /Volumes/dev_bronze/r_source_h/source_general/turbine_measurement (need to create folder in volume)
bronze_path = f"{path_volume}/{entity}" 
schema_path = f"{path_volume}/{entity}/_schema"
checkpoint_path = f"{path_volume}/{entity}/_checkpoint"

# COMMAND ----------

# DBTITLE 1,Import Modules
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

import utils.helper_config as hc
import utils.helper_silver as hs

# COMMAND ----------

# DBTITLE 1,Load Config
path_to_config = "../../config/tables/"
config = hc.load_yaml_config(file_path = f"{path_to_config}/{layer}/{source_system}/{entity}.yaml")
table_schema = config["target"]["table_schema"]
primary_keys = [col for col in table_schema if "is_primary_key" in table_schema[col] and table_schema[col]["is_primary_key"] == True]

# COMMAND ----------

# DBTITLE 1,Cleaning function
# Some sample cleaning steps
def clean(df):

    # 1. Apply schema and basic type conversions
    df = hs.apply_schema_types(df, config)

    # 2. Drop rows with null primary keys (turbine_id, timestamp) - can't identify the record
    pk_filter = F.lit(True)
    for pk in primary_keys:
        pk_filter = pk_filter & F.col(pk).isNotNull()
    df = df.filter(pk_filter)

    # 3. Remove outliers - drop readings outside 3 std devs from the per-turbine mean
    cols_of_interest = ["wind_speed", "wind_direction", "power_output"]
    for col in cols_of_interest:
        stats = df.groupBy("turbine_id").agg(
            F.mean(col).alias("mean"),
            F.stddev(col).alias("std")
        )
        df = (
            df.join(stats, "turbine_id")
              .filter(F.abs(F.col(col) - F.col("mean")) <= 3 * F.col("std"))
              .drop("mean", "std")
        )

    # 4. Impute remaining nulls with per-turbine rolling median
    w = Window.partitionBy("turbine_id").orderBy("timestamp").rowsBetween(-2, 2)
    for col in cols_of_interest:
        df = df.withColumn(col, F.when(
            F.col(col).isNull(),
            F.percentile_approx(col, 0.5).over(w)
        ).otherwise(F.col(col)))

    # 5. Drop duplicates — keep one record per (turbine_id, timestamp)
    df = df.dropDuplicates(primary_keys)

    return df

# COMMAND ----------

# DBTITLE 1,Upsert to Silver Function
def upsert_to_silver(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    cleaned = clean(batch_df)

    merge_condition = " AND ".join([f"t.{pk} = s.{pk}" for pk in primary_keys])

    (
        DeltaTable.forName(spark, table).alias("t")
        .merge(cleaned.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# DBTITLE 1,Main Script
(
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("header", "true")
        .load(bronze_path)
    .writeStream
        .foreachBatch(upsert_to_silver)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
)

# COMMAND ----------

# DBTITLE 1,Validate
display(spark.table(table).orderBy(primary_keys))
