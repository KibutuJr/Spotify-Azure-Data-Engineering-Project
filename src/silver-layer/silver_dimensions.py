# Databricks notebook source
# MAGIC %md
# MAGIC ### DimUser

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys

project_pth = os.path.join(os.getcwd(),'..', '..')

sys.path.append(project_pth)
from utils.transformations import reusable

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze-layer@kibutuazureproject.dfs.core.windows.net/DimUser")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **AUTOLOADER**

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver-layer@kibutuazureproject.dfs.core.windows.net/DimUser/checkpoint")\
        .option("schemaEvolutionMode", "addNewColumns")\
        .load("abfss://bronze-layer@kibutuazureproject.dfs.core.windows.net/DimUser")

display(df_user)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSORMATIONS
# MAGIC #### 1. Uniform user_name - All Caps

# COMMAND ----------

df_user = df_user.withColumn("user_name", upper(col("user_name")))
display(df_user)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. dropping rescued_data column

# COMMAND ----------

df_user_obj = reusable()

df_user = df_user_obj.dropColumns(df_user, ["_rescued_data"])
display(df_user)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Deduplication

# COMMAND ----------

df_user_obj = reusable()

df_user = df_user_obj.dropColumns(df_user, ["_rescued_data"])
df_user = df_user.dropDuplicates(["user_id"])
display(df_user)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Feature

# COMMAND ----------

df_user.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation", "abfss://silver-layer@kibutuazureproject.dfs.core.windows.net/DimUser/checkpoint")\
            .trigger(once=True)\
            .option("path", "abfss://silver-layer@kibutuazureproject.dfs.core.windows.net/DimUser/data")\
            .toTable("spotify_catalog.`silver-layer`.DimUser")

            


# COMMAND ----------

# MAGIC %md 
# MAGIC ### DimArtist
# MAGIC

# COMMAND ----------

df_art = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver-layer@kibutuazureproject.dfs.core.windows.net/DimArtist/checkpoint")\
    .option("schemaEvolutionMode", "addNewColumns")\
    .load("abfss://bronze-layer@kibutuazureproject.dfs.core.windows.net/DimArtist")
display(df_art)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSFORMATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop rescued_data column and Deduplications

# COMMAND ----------

df_art_obj = reusable()
df_art = df_art_obj.dropColumns(df_art,["_rescued_data"])
df_art = df_art.dropDuplicates(["artist_id"])
display(df_art)

# COMMAND ----------

df_art.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation", "abfss://silver-layer@kibutuazureproject.dfs.core.windows.net/DimArtist/checkpoint")\
            .trigger(once=True)\
            .option("path", "abfss://silver-layer@kibutuazureproject.dfs.core.windows.net/DimArtist/data")\
            .toTable("spotify_catalog.`silver-layer`.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimTrack

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver-layer@kibutuazureproject.dfs.core.windows.net/DimTrack/checkpoint")\
    .option("schemaEvolutionMode", "addNewColumns")\
    .load("abfss://bronze-layer@kibutuazureproject.dfs.core.windows.net/DimTrack/data")
display(df_track)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSFORMATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. create flag for duration_sec(<150 & <300)

# COMMAND ----------

df_track = df_track.withColumn("durationFlag",when(col('duration_sec')<150, "low")\
                               .when(col('duration_sec')<300, "medium")\
                               .otherwise("high"))

display(df_track)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. track_name changes (replace - with '  ')

# COMMAND ----------

df_track = df_track.withColumn("track_name",regexp_replace(col("track_name"), "-", ' '))
display(df_track)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. droping the reduced_data column

# COMMAND ----------

df_track = reusable().dropColumns(df_track,["_rescued_data"])
display(df_track)

# COMMAND ----------

df_track.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation", "abfss://silver-layer@kibutuazureproject.dfs.core.windows.net/DimTrack/checkpoint")\
            .trigger(once=True)\
            .option("path", "abfss://silver-layer@kibutuazureproject.dfs.core.windows.net/DimTrack/data")\
            .toTable("spotify_catalog.`silver-layer`.DimTrack")

            


# COMMAND ----------

# MAGIC %md
# MAGIC ### DimDate

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver-layer@kibutuazureproject.dfs.core.windows.net/DimDate/checkpoint")\
    .option("schemaEvolutionMode", "addNewColumns")\
    .load("abfss://bronze-layer@kibutuazureproject.dfs.core.windows.net/DimDate")
display(df_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSFORMATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ### drop the rescured_data column

# COMMAND ----------

df_date = reusable().dropColumns(df_date,["_rescued_data"])
display(df_date)

# COMMAND ----------

df_date.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation", "abfss://silver-layer@kibutuazureproject.dfs.core.windows.net/DimDate/checkpoint")\
            .trigger(once=True)\
            .option("path", "abfss://silver-layer@kibutuazureproject.dfs.core.windows.net/DimDate/data")\
            .toTable("spotify_catalog.`silver-layer`.DimDate")


# COMMAND ----------

# MAGIC %md
# MAGIC ### FactStrean

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver-layer@kibutuazureproject.dfs.core.windows.net/FactStream/checkpoint")\
    .option("schemaEvolutionMode", "addNewColumns")\
    .load("abfss://bronze-layer@kibutuazureproject.dfs.core.windows.net/FactStream")
display(df_fact)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSFORMATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ### drop the rescue_date
# MAGIC

# COMMAND ----------

df_fact = reusable().dropColumns(df_fact,["_rescued_data"])
display(df_fact)

# COMMAND ----------

df_fact.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation", "abfss://silver-layer@kibutuazureproject.dfs.core.windows.net/FactStream/checkpoint")\
            .trigger(once=True)\
            .option("path", "abfss://silver-layer@kibutuazureproject.dfs.core.windows.net/FactStream/data")\
            .toTable("spotify_catalog.`silver-layer`.FactStream")


# COMMAND ----------

