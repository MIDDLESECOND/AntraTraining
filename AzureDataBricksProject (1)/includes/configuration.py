# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC Define Data Paths.

# COMMAND ----------

# TODO
username = "Tianyuan_Zhang"

# COMMAND ----------

PipelinePath = f"/FP/{username}/dataengineering/classic/"

rawPath = PipelinePath + "raw/"
bronzePath = PipelinePath + "bronze/"
silverPath = PipelinePath + "silver/"
silverQuarantinePath = PipelinePath + "silverQuarantine/"
goldPath = PipelinePath + "gold/"

# COMMAND ----------

# MAGIC %md
# MAGIC Configure Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS FP_{username}")
spark.sql(f"USE FP_{username}")

# COMMAND ----------

# MAGIC %md
# MAGIC Import Utility Functions

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------


