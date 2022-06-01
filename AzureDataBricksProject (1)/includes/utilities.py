# Databricks notebook source

from pyspark.sql.session import SparkSession
from urllib.request import urlretrieve
from pyspark.sql.functions import from_unixtime, dayofmonth, month, hour
from delta import DeltaTable
from datetime import datetime
import time

CLASSIC_DATA = "classic_data_2020_h1.snappy.parquet"
CLASSIC_DELTA = "classic_data_2020_h1.delta"

# COMMAND ----------

def retrieve_data(file: str, landingPath: str) -> bool:
    """Download file from remote location to driver. Move from driver to DBFS."""

    base_url = "https://files.training.databricks.com/static/data/health-tracker/"
    url = base_url + file
    driverPath = "file:/databricks/driver/" + file
    dbfsPath = landingPath + file
    urlretrieve(url, file)
    dbutils.fs.mv(driverPath, dbfsPath)
    return True


def prepare_activity_data(landingPath) -> bool:
    retrieve_data(CLASSIC_DATA, landingPath)

    classicIngest = (
        spark.read.format("parquet")
        .load(landingPath + CLASSIC_DATA)
        .withColumn("time", from_unixtime("time"))
        .select(
            "*",
            dayofmonth("time").alias("day"),
            month("time").alias("month"),
            hour("time").alias("hour"),
        )
        .write.format("delta")
        .save(landingPath + CLASSIC_DELTA)
    )


def ingest_movie_data(path: str) -> bool:
    rawData = spark.read.option("multiline", "true").format("json").load()

    return True


def untilStreamIsReady(namedStream: str, progressions: int = 3) -> bool:
    queries = list(filter(lambda query: query.name == namedStream, spark.streams.active))
    while len(queries) == 0 or len(queries[0].recentProgress) < progressions:
        time.sleep(5)
        queries = list(filter(lambda query: query.name == namedStream, spark.streams.active))
    print("The stream {} is active and ready.".format(namedStream))
    return True

