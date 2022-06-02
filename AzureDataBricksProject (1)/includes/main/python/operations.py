# Databricks notebook source

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
)
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

# COMMAND ----------

def read_movie(rawPath: str) -> DataFrame:
  df = spark.read.format("json").option("multiline", "true").load(rawPath)
  return df.select(explode("movie"))

# COMMAND ----------

def raw_meta(rawDF: DataFrame) -> DataFrame:
  bronzeDF = rawDF.select(col("col").alias("movie"),
                          lit("json files").alias("dataSource"),
                          current_timestamp().alias("timeRetrieved"),
                          lit("new").alias("status"),
                          current_timestamp().cast("date").alias("p_date")
                         ).withColumn("uniqueID",monotonically_increasing_id())
  return bronzeDF

# COMMAND ----------

def batch_writer(bronzeDF: DataFrame, partition_column: str, exclude_columns: List =[], mode: str = "append") -> DataFrame:
  NbronzeDF = bronzeDF.drop(*exclude_columns).write.format("delta").mode(mode).partitionBy(partition_column)
  return NbronzeDF

# COMMAND ----------

def read_batch_bronze() -> DataFrame:
    return spark.read.format("delta").load(bronzePath).filter("status = 'new'")

# COMMAND ----------

def explode_bronze(bronzeDF: DataFrame) -> DataFrame:
  df = bronzeDF.select("*", "movie.*")
  return df

# COMMAND ----------

def run_time_issue(df: DataFrame) -> (DataFrame, DataFrame):
  df.withColumn("status", when((df.RunTime <= 0), "quarantine").otherwise(df.status))
  df.withColumn("RunTime", when((df.RunTime <= 0), (df.RunTime * -1)).otherwise(df.RunTime))
  quara_df = df.filter(df.status == "quarantine")
  clean_df = df.filter(df.status != "quarantine")
  return (clean_df, quara_df)

# COMMAND ----------

def seperate_tables(df: DataFrame) -> (DataFrame, DataFrame, DataFrame):
  main_df = df.select(col("movie"),
                     col("dataSource"),
                     col("timeRetrieved"),
                     col("status"),
                     col("p_date"),
                     col("uniqueID"),
                     col("BackdropUrl"),
                     col("Budget"),
                     col("CreatedBy"),
                     col("CreatedDate").cast("date"),
                     col("id"),
                     col("ImdbUrl"),
                     col("Overview"),
                     col("PosterUrl"),
                     col("Price"),
                     col("ReleaseDate").cast("date"),
                     col("Revenue"),
                     col("RunTime"),
                     col("Tagline"),
                     col("Title"),
                     col("TmdbUrl"),
                     col("UpdatedBy"),
                     col("UpdatedDate").cast("date")
                    )
  main_df = maindf.dropDuplicates("id")
  genre_df = df.select(explode(col("genres")), col("id").alias("movie_id"))
  genre_df = genre_df.select("col.*", "movie_id")
  oglan_df = df.select(col("OriginalLanguage"), col("id"))
  return (main_df, genre_df, oglan_df)

# COMMAND ----------

def fix_budget(df:DataFrame) -> (DataFrame):
  df.withColumn("Budget", when((df.Budget < 1000000), 1000000).otherwise(df.Budget))
  return df

# COMMAND ----------


