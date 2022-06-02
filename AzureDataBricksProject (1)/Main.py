# Databricks notebook source
from pyspark.sql.functions import *
from delta import DeltaTable
from datetime import datetime

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

#dbutils.fs.ls("FP/Tianyuan_Zhang/dataengineering/")
#dbutils.fs.rm(silverPath, recurse=True)

# COMMAND ----------

rawPath = "dbfs:/FileStore/tables/FP"

# COMMAND ----------

rawDF = read_movie(rawPath)
display(rawDF)

# COMMAND ----------

bronzeDF = raw_meta(rawDF)
display(bronzeDF)
type(bronzeDF)

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

display(df)

# COMMAND ----------

df = explode_bronze(df)
display(df)

# COMMAND ----------

clean_df, quara_df = run_time_issue(df)
display(clean_df)

# COMMAND ----------

maindf, gendf, landf = seperate_tables(clean_df)
display(gendf)

# COMMAND ----------

maindf = fix_budget(maindf)
maindf.select(min("Budget")).collect()

# COMMAND ----------

display(maindf)

# COMMAND ----------

genre_table = batch_writer (gendf, "id")
language_table = batch_writer(landf, "OriginalLanguage")
genre_table.save(silverPath + "genres/")
language_table.save(silverPath + "langauge/")

# COMMAND ----------

display(dbutils.fs.ls(silverPath))

# COMMAND ----------


