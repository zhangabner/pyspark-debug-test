"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import json
import unittest

import pyspark.sql.functions as F
import quinn
from dependencies.spark import start_spark
from jobs.udffunctions import *
from pyspark.sql.functions import mean
from pyspark.sql.types import *

from chispa import assert_column_equality, assert_approx_column_equality
import pyspark.sql.functions as F
from pyspark.sql.types import *
from chispa import assert_df_equality, assert_approx_df_equality

from quinn.extensions import *



def title(x,y):
   if y:
       x = x.title()
   return x



class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.config = json.loads("""{"steps_per_floor": 21}""")
        self.spark, *_ = start_spark()
        self.test_data_path = 'test_data/'

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_udf(self):
        df = self.spark.createDataFrame([
            ["aaa","1"],
            ["bbb","2"],
            ["ccc","5"]
        ]).toDF("text","id")

        title_udf = F.udf(title, StringType())
        self.spark.udf.register('title_udf', title_udf)

        df.withColumn('text_title',title_udf('text',F.lit(True)))#.show()


    # def test_flatmap(self):
    #     df = self.spark.read.text("resources/words.txt")
    #     words = df.rdd.flatMap(lambda row: row[0].split(" ")).collect()
    #     print(words)



    def test_pandas_approach(self):
        df = self.spark.createDataFrame([(1, 5), (2, 9), (3, 3), (4, 1)], ["mvv", "count"])
        mvv = list(df.select('mvv').toPandas()['mvv'])
        assert mvv == [1, 2, 3, 4]


    def test_flatmap_collect(self):
        df = self.spark.createDataFrame([(1, 5), (2, 9), (3, 3), (4, 1)], ["mvv", "count"])
        mvv = df.select('mvv').rdd.flatMap(lambda x: x).collect()
        assert mvv == [1, 2, 3, 4]


    def test_flatmap_toLocalIterator(self):
        df = self.spark.createDataFrame([(1, 5), (2, 9), (3, 3), (4, 1)], ["mvv", "count"])
        mvv = list(df.select('mvv').rdd.flatMap(lambda x: x).toLocalIterator())
        assert mvv == [1, 2, 3, 4]


    def test_rdd_map(self):
        df = self.spark.createDataFrame([(1, 5), (2, 9), (3, 3), (4, 1)], ["mvv", "count"])
        mvv = df.select('mvv').rdd.map(lambda row : row[0]).collect()

        assert mvv == [1, 2, 3, 4]


    def test_list_comprehension_map(self):
        df = self.spark.createDataFrame([(1, 5), (2, 9), (3, 3), (4, 1)], ["mvv", "count"])
        mvv = [row[0] for row in df.select('mvv').collect()]
        assert mvv == [1, 2, 3, 4]


    def test_list_comprehension_toLocalIterator(self):
        df = self.spark.createDataFrame([(1, 5), (2, 9), (3, 3), (4, 1)], ["mvv", "count"])
        mvv = [r[0] for r in df.select('mvv').toLocalIterator()]
        assert mvv == [1, 2, 3, 4]


    def test_pandas_to_two_lists(self):
        df = self.spark.createDataFrame([(1, 5), (2, 9), (3, 3), (4, 1)], ["mvv", "count"])
        collected = df.select('mvv', 'count').toPandas()
        mvv = list(collected['mvv'])
        count = list(collected['count'])
        assert mvv == [1, 2, 3, 4]
        assert count == [5, 9, 3, 1]

    def test_cast_arraytype(self):
        data = [
            (['200', '300'], [200, 300]),
            (['400'], [400]),
            (None, None)
        ]
        df = self.spark.createDataFrame(data, ["nums", "expected"])\
            .withColumn("actual", F.col("nums").cast(ArrayType(IntegerType(), True)))
        assert_column_equality(df, "actual", "expected")

    def test_approx_df_equality_same(self):
        data1 = [
            (1.1, "a"),
            (2.2, "b"),
            (3.3, "c"),
            (None, None)
        ]
        df1 = self.spark.createDataFrame(data1, ["num", "letter"])

        data2 = [
            (1.05, "a"),
            (2.13, "b"),
            (3.3, "c"),
            (None, None)
        ]
        df2 = self.spark.createDataFrame(data2, ["num", "letter"])

        assert_approx_df_equality(df1, df2, 0.1)


    def test_approx_df_equality_different(self):
        data1 = [
            (1.1, "a"),
            (2.2, "b"),
            (3.3, "c"),
            (None, None)
        ]
        df1 = self.spark.createDataFrame(data1, ["num", "letter"])

        data2 = [
            (1.1, "a"),
            (5.0, "b"),
            (3.3, "z"),
            (None, None)
        ]
        df2 = self.spark.createDataFrame(data2, ["num", "letter"])

        assert_approx_df_equality(df1, df2, 0.1)


    def test_schema_mismatch_error(self):
        data1 = [
            (1, "a"),
            (2, "b"),
            (3, "c"),
            (None, None)
        ]
        df1 = self.spark.createDataFrame(data1, ["num", "letter"])

        data2 = [
            (1, 6),
            (2, 7),
            (3, 8),
            (None, None)
        ]
        df2 = self.spark.createDataFrame(data2, ["num", "num2"])

        assert_df_equality(df1, df2)



    def test_example_error(self):
        # schema = StructType([StructField("country.name", StringType(), True)])
        df = self.spark.createDataFrame(
            [("china", "asia"), ("colombia", "south america")],
            ["country.name", "continent"]
        )
        df.select("`country.name`").show()

    def test_random_value_from_array(self):
        df = self.spark.createDataFrame(
            [
                (['a', 'b', 'c'],),
                (['a', 'b', 'c', 'd'],),
                (['x'],),
                ([None],)
            ],
            [
                "letters"
            ]
        )
        # df.show()
        actual_df = df.withColumn(
            "random_letter",
            quinn.array_choice(F.col("letters"))
        )
        # actual_df.show()


    def test_random_value_from_columns(self):
        df = self.spark.createDataFrame(
            [
                (1, 2, 3),
                (4, 5, 6),
                (7, 8, 9),
                (10, None, None),
                (None, None, None)
            ],
            ["num1", "num2", "num3"]
        )
        # df.show()
        actual_df = df.withColumn(
            "random_number",
            quinn.array_choice(F.array(F.col("num1"), F.col("num2"), F.col("num3")))
        )
        # actual_df.show()


    def test_random_animal(self):
        df = self.spark.createDataFrame([('jose',), ('maria',), (None,)], ['first_name'])
        cols = list(map(lambda col_name: F.lit(col_name), ['cat', 'dog', 'mouse']))
        actual_df = df.withColumn(
            "random_animal",
            quinn.array_choice(F.array(*cols))
        )
        # actual_df.show()


# question motivation: https://stackoverflow.com/questions/63103302/creating-dictionary-from-pyspark-dataframe-showing-outofmemoryerror-java-heap-s/63103739#63103739
    def test_to_dictionary(self):
        data = [
            ("BOND-9129450", "90cb"),
            ("BOND-1742850", "d5c3"),
            ("BOND-3211356", "811f"),
            ("BOND-7630290", "d5c3"),
            ("BOND-7175508", "90cb"),
        ]
        df = self.spark.createDataFrame(data, ["id", "hash_of_cc_pn_li"])
        agg_df = df.groupBy("hash_of_cc_pn_li").agg(F.max("hash_of_cc_pn_li").alias("hash"), F.collect_list("id").alias("id"))
        res = quinn.two_columns_to_dictionary(agg_df, "hash", "id")
        print(res)


if __name__ == '__main__':
    unittest.main()
