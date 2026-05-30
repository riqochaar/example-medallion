# Databricks notebook source
# MAGIC %md # Gold Layer
# MAGIC Daily summary statistics and anomaly flags per turbine

# COMMAND ----------

# DBTITLE 1,Set Parameters
dbutils.widgets.text("env", "dev")

# COMMAND ----------

# DBTITLE 1,Get Parameters
env = dbutils.widgets.get("env")

# COMMAND ----------

# DBTITLE 1,Define Config
layer = "gold"
data_product = "data_product"
entity = "fct_turbine"
table_gold = f"{env}_{layer}.c_{data_product}_h.{entity}"

# COMMAND ----------

# DBTITLE 1,Import Modules
from pyspark.sql import functions as F
from delta.tables import DeltaTable

import src.notebooks.utils.helper_config as hc

# COMMAND ----------

# DBTITLE 1,Load Config
path_to_config = "../../../config/tables/"
config = hc.load_yaml_config(file_path=f"{path_to_config}/{layer}/{data_product}/{entity}.yaml")

# Get target table information
table_schema = config["target"]["table_schema"]
primary_keys = [
    col for col in table_schema 
    if "is_primary_key" in table_schema[col] 
    and table_schema[col]["is_primary_key"] == True
]

# Get source table information
table_sources = config["sources"][0] # Get first table source as an example
table_silver = f"{env}_{table_sources['catalog']}.{table_sources['schema']}.{table_sources['name']}"

# COMMAND ----------

# DBTITLE 1,Build Summary
df_silver = spark.table(table_silver)

df_gold = spark.sql(f"""
    WITH summary AS (
        SELECT
            turbine_id,
            DATE(timestamp)        AS date,
            MIN(power_output)      AS min_power,
            MAX(power_output)      AS max_power,
            AVG(power_output)      AS avg_power,
            STDDEV(power_output)   AS stddev_power
        FROM {table_silver}
        GROUP BY turbine_id, DATE(timestamp)
    ),
    daily_stats AS (
        SELECT
            date,
            AVG(avg_power)    AS global_mean,
            STDDEV(avg_power) AS global_std
        FROM summary
        GROUP BY date
    )
    SELECT
        s.*,
        ABS(s.avg_power - d.global_mean) > 2 * d.global_std AS is_anomaly
    FROM summary s
    JOIN daily_stats d ON s.date = d.date
""")

# COMMAND ----------

# DBTITLE 1,Merge into Gold
merge_condition = " AND ".join([f"t.{pk} = s.{pk}" for pk in primary_keys])

(
    DeltaTable.forName(spark, table_gold).alias("t")
    .merge(df_gold.alias("s"), merge_condition)
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

# DBTITLE 1,Validate
display(spark.table(table_gold).orderBy("date", "turbine_id"))
