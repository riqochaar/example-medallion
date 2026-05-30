# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Table Creation
# MAGIC This notebook contains SQL scripts to create all required tables in the `*env*_*layer*` schemas
# MAGIC

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()  

# COMMAND ----------

# DBTITLE 1,Import Modules
import os
import utils.helper_config as hc

# COMMAND ----------

# DBTITLE 1,Set Parameters
dbutils.widgets.text("env", "")

# COMMAND ----------

# DBTITLE 1,Get parameters
env = dbutils.widgets.get("env")

# COMMAND ----------

path_to_config = "../../config"
path_to_config_tables = f"{path_to_config}/tables"

# COMMAND ----------

# DBTITLE 1,Get All Config Source Files
config_paths = hc.list_all_files_in_directory(path_to_config_tables)

# COMMAND ----------

# DBTITLE 1,Dynamic SQL to Create Table
for cp in config_paths:

    layer = cp.split('/')[-3]
    catalog = f"{env}_{layer}"
    data_asset = cp.split("/")[-2]
    if layer == "silver":
        schema = f"b_{data_asset}_h"
    elif layer == "gold":
        schema = f"c_{data_asset}_h"
    table_name = cp.split("/")[-1].split(".")[0]

    print("-" * 100)
    print(f"Creating table for {table_name}\n")

    # Load in config
    config_table = hc.load_yaml_config(file_path = cp)["target"]

    # Get table schema
    table_schema = config_table['table_schema']
    columns = list(table_schema.keys())

    # Create SQL statement for columns
    columns_sql = []
    for idx, c in enumerate(columns):

        # Check if primary key
        if "is_primary_key" in table_schema[c]:
            if table_schema[c]["is_primary_key"] == True:
                is_primary_key = True
            else:
                is_primary_key = False
        else:
            is_primary_key = False

        # Get items to build SQL column statement
        tab = "    " if idx > 0 else "" 
        comma = "," if idx < len(columns) - 1 else ""
        null = " NOT NULL" if is_primary_key else ""
        column_type = table_schema[c]["type"].upper()

        # Build SQL column statement
        column_sql = f"{tab}{c} {column_type}{null}{comma}"
        columns_sql.append(column_sql)

    columns_sql = "\n".join(columns_sql)

    # SQL CREATE TABLE statement
    sql_statement = f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} (
    {columns_sql}
)
USING DELTA
"""

    print(f"Executing SQL command:\n{sql_statement}")
    spark.sql(sql_statement)
