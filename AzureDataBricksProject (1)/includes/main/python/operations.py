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
                         )
  return bronzeDF

# COMMAND ----------

def batch_writerter(bronzeDF: DataFrame, partition_column: str, exclude_columns: List =[], mode: str = "append") -> DataFrame:
  NbronzeDF = bronzeDF.drop(*exclude_columns).write.format("delta").mode(mode).partitionBy(partition_columns)
  return NbronzeDF

# COMMAND ----------

def read_batch_bronze() -> DataFrame:
    return spark.read.format("delta").load(bronzePath).filter("status = 'new'")

# COMMAND ----------

def transform_bronze(bronze: DataFrame, quarantine: bool = False) -> DataFrame:

    json_schema = """
      BackdropUrl STRING,
      Budget INTEGER,
      CreatedBy STRING,
      CreatedDate TIMESTAMP,
      Id INTEGER,
      ImdbUrl STRING,
      OriginalLanguage STRING,
      Overview STRING,
      PosterUrl STRING,
      Price FLOAT
      ReleaseDate TIMESTAMP
      Revenue INTEGER
      Runtime INTEGER
      Tagline STRING
      Title STRING
      TmdbUrl STRING
      UpdatedBy STRING
      UpdatedDate TIMESTAMP
      genres ARRAY
  """

    bronzeAugmentedDF = bronze.distinct().withColumn(
        "nested_json", from_json(col("movie"), json_schema)
    )
    
    # quarantine and fix the run time
    
    bronzeAugmentedDF.withcolumn("Runtime",
                                when(bronzeAugmentedDF.Runtime >= 0, bronzeAugmentedDF.Runtime).otherwise(bronzeAugmentedDF.Runtime * -1))

    silver_movie = bronzeAugmentedDF.select("movie", "nested_json.*")

    if not quarantine:
        silver_movie = silver_movie.select(
            "movie",
            col("device_id").cast("integer").alias("device_id"),
            "steps",
            col("time").alias("eventtime"),
            "name",
            col("time").cast("date").alias("p_eventdate"),
        )
    else:
        silver_movie = silver_movie.select(
            "value",
            "device_id",
            "steps",
            col("time").alias("eventtime"),
            "name",
            col("time").cast("date").alias("p_eventdate"),
        )

    return silver_movie
