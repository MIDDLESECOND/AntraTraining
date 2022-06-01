# Databricks notebook source
from pyspark.sql.functions import *
from delta import DeltaTable
from datetime import datetime

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

rawPath = "dbfs:/FileStore/tables/FP"

# COMMAND ----------

rawDF = read_movie(rawPath)
display(rawDF)

# COMMAND ----------

bronzeDF = raw_meta(rawDF)
display(bronzeDF)

# COMMAND ----------

NbronzeDF = batch_writer(bronzeDF, partition_column = "p_date")
NbronzeDF.save(bronzePath)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movie_bronze
"""
)

spark.sql(
    f"""
CREATE TABLE movie_bronze
USING DELTA
LOCATION "{bronzePath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

dbutils.fs.rm(rawPath, recurse=True)

# COMMAND ----------

df = read_batch_bronze()
display(df)

# COMMAND ----------


