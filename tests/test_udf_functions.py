"""
test_udf_functions.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in udf_functions.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import json
import unittest

import pyspark.sql.functions as F
import quinn
from chispa import (assert_approx_column_equality, assert_approx_df_equality,
                    assert_column_equality, assert_df_equality)
from dependencies.spark import start_spark
from jobs.udf_functions import (change_column_names,modify_column_names, divide_by_three,divide_by_zero,
                               dots_to_underscores, remove_non_word_characters,
                               sort_columns)
from pyspark.sql.functions import mean
from pyspark.sql.types import *
from quinn.extensions import *


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

    def test_remove_non_word_characters_testdata1(self):
        data = [
            ("jo&&se", "jose"),
            ("**li**", "li"),
            ("#::luisa", "luisa"),
            (None, None)
        ]
        df = self.spark.createDataFrame(data, ["name", "expected_name"])\
            .withColumn("clean_name", remove_non_word_characters(F.col("name")))
        assert_column_equality(df, "clean_name", "expected_name")


    def test_remove_non_word_characters_testdata2_error(self):
        data = [
            ("matt7", "matt"),
            ("bill&", "bill"),
            ("isabela*", "isabela"),
            (None, None)
        ]
        df = self.spark.createDataFrame(data, ["name", "expected_name"])\
            .withColumn("clean_name", remove_non_word_characters(F.col("name")))
        # with pytesraises(ColumnsNotEqualError) as e_info:
        assert_column_equality(df, "clean_name", "expected_name")


    # def test_divide_by_three(self):
    #     data = [
    #         (1, 0.33),
    #         (2, 0.66),
    #         (3, 1.0),
    #         (None, None)
    #     ]
    #     df = self.spark.createDataFrame(data, ["num", "expected"])\
    #         .withColumn("num_divided_by_three", divide_by_three(F.col("num")))
    #     assert_approx_column_equality(df, "num_divided_by_three", "expected", 0.01)

    # def test_divide_by_zero(self):
    #     data = [
    #         (1, 0.33),
    #         (2, 0.66),
    #         (3, 1.0),
    #         (None, None)
    #     ]
    #     df = self.spark.createDataFrame(data, ["num", "expected"])\
    #         .withColumn("num_divided_by_zero", divide_by_zero(F.col("num")))
    #     assert_approx_column_equality(df, "num_divided_by_zero", "expected", 0.01)

    # def test_divide_by_three_error(self):
    #     data = [
    #         (5, 1.66),
    #         (6, 2.0),
    #         (7, 4.33),
    #         (None, None)
    #     ]
    #     df = self.spark.createDataFrame(data, ["num", "expected"])\
    #         .withColumn("num_divided_by_three", divide_by_three(F.col("num")))
    #     assert_approx_column_equality(df, "num_divided_by_three", "expected", 0.01)


    def test_sort_columns_asc(self):
        source_data = [
            ("jose", "oak", "switch"),
            ("li", "redwood", "xbox"),
            ("luisa", "maple", "ps4"),
        ]
        source_df = self.spark.createDataFrame(source_data, ["name", "tree", "gaming_system"])

        actual_df = sort_columns(source_df, "asc")

        expected_data = [
            ("switch", "jose", "oak"),
            ("xbox", "li", "redwood"),
            ("ps4", "luisa", "maple"),
        ]
        expected_df = self.spark.createDataFrame(expected_data, ["gaming_system", "name", "tree"])

        assert_df_equality(actual_df, expected_df)


    def test_sort_columns_desc(self):
        source_data = [
            ("jose", "oak", "switch"),
            ("li", "redwood", "xbox"),
            ("luisa", "maple", "ps4"),
        ]
        source_df = self.spark.createDataFrame(source_data, ["name", "tree", "gaming_system"])

        actual_df = sort_columns(source_df, "desc")

        expected_data = [
            ("oak", "jose", "switch"),
            ("redwood", "li", "xbox"),
            ("maple", "luisa", "ps4"),
        ]
        expected_df = self.spark.createDataFrame(expected_data, ["tree", "name", "gaming_system"])

        assert_df_equality(actual_df, expected_df)


    def test_change_column_names(self):
        source_data = [
            ("jose", "oak", "switch")
        ]
        source_df = self.spark.createDataFrame(source_data, ["some first name", "some.tree.type", "a gaming.system"])
        actual_df = change_column_names(source_df)
        expected_data = [
            ("jose", "oak", "switch")
        ]
        expected_df = self.spark.createDataFrame(expected_data, ["some_&first_&name", "some_$tree_$type", "a_&gaming_$system"])
        assert_df_equality(actual_df, expected_df)

    def test_modify_column_names_error(self):
        source_data = [
            ("jose", 8),
            ("li", 23),
            ("luisa", 48),
        ]
        source_df = self.spark.createDataFrame(source_data, ["first.name", "person.favorite.number"])

        actual_df = modify_column_names(source_df, dots_to_underscores)

        expected_data = [
            ("jose", 8),
            ("li", 23),
            ("luisa", 48),
        ]
        expected_df = self.spark.createDataFrame(expected_data, ["first_name", "person_favorite_number"])

        assert_df_equality(actual_df, expected_df)

if __name__ == '__main__':
    unittest.main()
