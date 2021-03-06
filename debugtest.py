from pyspark import SparkFiles
from pyspark.sql import Row, SparkSession, DataFrame

test_data = [
    {
        "ID": 1,
        "First_Name": "Bob",
        "Last_Name": "Builder",
        "Age": 24
    },
    {
        "ID": 2,
        "First_Name": "Sam",
        "Last_Name": "Smith",
        "Age": 41
    }
]
app_name='my_spark_app'
master='local[*]'

spark_builder = (
    SparkSession
    .builder
    .master(master)
    .appName(app_name))

spark = spark_builder.getOrCreate()
test_df = spark.createDataFrame(map(lambda x: Row(**x), test_data))
test_df.show(1)