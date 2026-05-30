# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Object Creation
# MAGIC This notebook contains SQL scripts to create all required objects in the Unity Catalog
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Catalogs
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS dev_bronze;
# MAGIC CREATE CATALOG IF NOT EXISTS dev_silver;
# MAGIC CREATE CATALOG IF NOT EXISTS dev_gold;

# COMMAND ----------

# DBTITLE 1,Create Schemas
# MAGIC %sql
# MAGIC -- Bronze schema
# MAGIC CREATE SCHEMA IF NOT EXISTS dev_bronze.r_source_h;
# MAGIC
# MAGIC -- Silver schema
# MAGIC CREATE SCHEMA IF NOT EXISTS dev_silver.b_source_h;
# MAGIC
# MAGIC -- Gold schema
# MAGIC CREATE SCHEMA IF NOT EXISTS dev_gold.c_data_product_h;

# COMMAND ----------

# DBTITLE 1,Create Volumes
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS dev_bronze.r_source_h.source_general;

# COMMAND ----------

# DBTITLE 1,Create Volume Directories
dbutils.fs.mkdirs("/Volumes/dev_bronze/r_source_h/source_general/turbine_measurement")
