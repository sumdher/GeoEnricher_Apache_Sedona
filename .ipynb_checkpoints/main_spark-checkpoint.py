from pyspark.sql import SparkSession
from sedona.spark import *

config = SedonaContext.builder() .\
    config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.7.0,'
           'org.datasyslab:geotools-wrapper:1.7.0-28.5'). \
    getOrCreate()

sedona = SedonaContext.create(config)

italy_cens_path = "./data_Italy/merged/merged_pop_geom/"

italy_df = sedona.read.format("shapefile").load(italy_cens_path)
italy_df.createOrReplaceTempView("it")
italy_df.show()