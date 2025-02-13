from pyspark.sql import SparkSession
from sedona.spark import *
from pyspark.sql.functions import *
import time
import pandas as pd

def restart_sedona():
    sedona.stop()
    sedona = SedonaContext.create(config)
    print("Restarting Sedona", end="", flush=True)
    while not SparkSession.builder.getOrCreate().sparkContext._jsc.sc().isStarted():
        print(".", end="", flush=True)
        time.sleep(1)
    print("\n Sedona up!\n")
    
config = SedonaContext.builder() \
    .config("spark.executor.memory", "15g") \
    .config("spark.driver.memory", "10g") \
    .config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.7.0,'
           'org.datasyslab:geotools-wrapper:1.7.0-28.5') \
    .getOrCreate()

sedona = SedonaContext.create(config)
time.sleep(10)

cores = sedona.sparkContext.defaultParallelism

print("\n Reading files...")
italy_cens_path = "./data_Italy/merged/merged_pop_geom/"
EU_cens_path = "./data_EU/census_grid_EU/grids.parquet"
germ_path = "./data_Germany/germ_points.csv"
contr_path = "./data_EU/countries_shp/"

contr_df = sedona.read.format("shapefile").load(contr_path)
germ_df = sedona.read.format("csv").option("delimiter", ";").option("header", "true").load(germ_path)
germ_df = germ_df.withColumn("geom", expr("ST_Point(x_mp_100m, y_mp_100m)"))
eu_df = sedona.read.format("geoparquet").option("legacyMode", "true").load(EU_cens_path)
italy_df = sedona.read.format("shapefile").load(italy_cens_path)
print("Reading files DONE!\n")

# DataFrame for results
res_df = pd.DataFrame()
j = -1

for c in [1, cores, cores * 2]:
    if c == 1:
        print("Using default partitioning.\n")
    else:
        print(f"Repartitioning to {c} partitions...")
        eu_df = eu_df.repartition(c)
        germ_df = germ_df.repartition(c)
        italy_df = italy_df.repartition(c)
        print("Repartitioning DONE!\n")

    for i in range(10):
        j += 1

        # print(f"Self-join Italy (Iteration {i+1}, Partitions: {c})...\n")
        # res = italy_df.alias("a").join(italy_df.alias("b"), expr("ST_Intersects(a.geometry, b.geometry)"))
        # start = time.time()
        # print(f"Result: {res.count():,} rows")
        # end = time.time()
        # res_df.loc[j, f'it_it_{c}'] = end - start
        # print(f"Time elapsed: {end - start:.2f} seconds\n")

        # restart_sedona()

        print(f"Self-join EU (Iteration {i+1}, Partitions: {c})...\n")
        res = eu_df.alias("a").join(eu_df.alias("b"), expr("ST_Intersects(a.geom, b.geom)"))
        start = time.time()
        print(f"Result: {res.count():,} rows")
        end = time.time()
        res_df.loc[j, f'eu_eu_{c}'] = end - start
        print(f"Time elapsed: {end - start:.2f} seconds\n")

        # restart_sedona()

print("\n Benchmarking completed.\n")
res_df.to_csv("results.csv", index=False)






























































from pyspark.sql import SparkSession
from sedona.spark import *
from pyspark.sql.functions import *
import time

config = SedonaContext.builder() .\
    config("spark.executor.memory", "2g") .\
    config("spark.driver.memory", "5g") .\
    config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.7.0,'
           'org.datasyslab:geotools-wrapper:1.7.0-28.5'). \
    getOrCreate()

sedona = SedonaContext.create(config)
# sedona.sparkContext.setLogLevel("OFF")
cores = sedona.sparkContext.defaultParallelism

cens_shp_path = "/opt/workspace/my_data/Dati Use Case/Sezioni_censimento_Lombardia_R03_21/SHP/"

cens_df = sedona.read.format("shapefile").load(cens_shp_path)

from sedona.utils.adapter import Adapter
from sedona.core.enums import GridType, IndexType

def sp_part_index(df, col="geom"):
    rdd = Adapter.toSpatialRdd(df, col)
    rdd.analyze()
    rdd.spatialPartitioning(GridType.QUADTREE)
    rdd.buildIndex(IndexType.QUADTREE, buildIndexOnSpatialPartitionedRDD=True)
    return Adapter.toDf(rdd, sedona)
import time

res = cens_df.alias("a").join(cens_df.alias("b"), expr("ST_Intersects(a.geometry, b.geometry)"))

start = time.time()
print(f"Result: {res.count():,}")
end = time.time()

if end - start <= 60:
    print(f"Elapsed time: {(end-start):.2f} sec")
else:
    print(f"Elapsed time: {(end-start)/60:.2f} min")
    
