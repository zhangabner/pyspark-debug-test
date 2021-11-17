from pyspark.sql import SparkSession

appName = "PySpark Partition Example"
master = "local[8]"

# Create Spark session with Hive supported.
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

configurations = spark.sparkContext.getConf().getAll()
for conf in configurations:
    print(conf)
