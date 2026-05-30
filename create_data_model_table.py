# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Table Creation: Dimensions & Facts
# MAGIC _This notebook contains SQL scripts to create all required tables in the `*env*_gold` schema, including both dimension and fact tables._
# MAGIC

# COMMAND ----------

# DBTITLE 1,Read function to find Wheel file location
# MAGIC %run "../../03_utils/wheel_utilities/wheel_path_finder"

# COMMAND ----------

# DBTITLE 1,Find Wheel file location
# Detect repo root (no matter where this notebook is)
notebook_dir = Path(os.getcwd())
repo_root = find_repo_root(notebook_dir, "transformation/dist")

dist_dir = repo_root / "transformation" / "dist"

# Find wheel files
wheel_files = list(dist_dir.glob("*.whl"))
latest_wheel = max(wheel_files, key=lambda f: f.stat().st_mtime)

wheel_path = f"/Workspace{latest_wheel}"

print(f"Installing latest wheel: {wheel_path}")
%pip install --force-reinstall "$wheel_path"

# COMMAND ----------

dbutils.library.restartPython()  

# COMMAND ----------

import os
from carbon.utils import helper_config as hc

# COMMAND ----------

path_to_config = "../config/"
path_to_config_tables = f"{path_to_config}/tables"

catalog_prefix = 'prod_'  ##CHANGE TO proto_ IF RUNNING IN INFOLAB OR PROTO
if catalog_prefix == 'infolab':
    catalog = f"{catalog_prefix}"
    schema = "c_carbon_emissions_h"
else:       
    catalog = f"{catalog_prefix}gold"
    schema = "c_carbon_emissions_h"

# COMMAND ----------

configs = os.listdir(path_to_config_tables)

for c in configs:

    table_name = c.split(".")[0]
    print("-" * 100)
    print(f"Creating table for {table_name}\n")

    # Load in config
    config_table = hc.load_yaml_config(file_name = table_name)["target"]

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
