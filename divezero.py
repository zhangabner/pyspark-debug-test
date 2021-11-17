from pyspark.sql import SparkSession
import pyspark.sql.functions as F

appName = "PySpark Partition Example"
master = "local[8]"

# Create Spark session with Hive supported.
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

def divide_by_zero(col):
    return col / 0

def test_rdd_map_divezero():
    data = [
        (1, 0.33),
        (2, 0.66),
        (3, 1.0),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["num", "expected"])
    
    df2 =df.withColumn("num_divided_by_three", divide_by_zero(F.col("num")))

    return df2
df=test_rdd_map_divezero()
df.show(4)
