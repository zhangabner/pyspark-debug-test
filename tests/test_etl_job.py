"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest

import json

from pyspark.sql.functions import mean

from dependencies.spark import start_spark
from jobs.etl_job import transform_data,extract_data
from jobs.udf_functions import (change_column_names,modify_column_names, divide_by_three,divide_by_zero,
                               dots_to_underscores, remove_non_word_characters,
                               sort_columns)
from chispa import (assert_approx_column_equality, assert_approx_df_equality,
                    assert_column_equality, assert_df_equality)


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

    def test_transform_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble
        input_data = (
            self.spark
            .read
            .parquet(self.test_data_path + 'employees'))

        expected_data = (
            self.spark
            .read
            .parquet(self.test_data_path + 'employees_report'))

        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()
        expected_avg_steps = (
            expected_data
            .agg(mean('steps_to_desk').alias('avg_steps_to_desk'))
            .collect()[0]
            ['avg_steps_to_desk'])

        # act
        data_transformed = transform_data(input_data, 21)

        cols = len(expected_data.columns)
        rows = expected_data.count()
        avg_steps = (
            expected_data
            .agg(mean('steps_to_desk').alias('avg_steps_to_desk'))
            .collect()[0]
            ['avg_steps_to_desk'])

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertEqual(expected_avg_steps, avg_steps)
        self.assertTrue([col in expected_data.columns
                         for col in data_transformed.columns])

    def test_extract_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble
        input_data = (
            self.spark
            .read
            .parquet(self.test_data_path + 'employees'))

        data = extract_data(self.spark)

        # assert
        self.assertTrue([col in input_data.columns
                         for col in data.columns])

    def test_extract_data_new(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble
        input_data = (
            self.spark
            .read
            .parquet('/home/u18/test_data/employees_age'))

        data = extract_data(self.spark)
        
        expected_df = sort_columns(input_data, "desc")

        actual_df = sort_columns(data, "desc")
        # assert
        assert_df_equality(actual_df, expected_df)
        # print(input_data.columns,data.columns)
        # self.assertTrue([col in input_data.columns
        #                  for col in data.columns]) 
        # self.assertTrue([col in data.columns
        #                  for col in input_data.columns]) 

if __name__ == '__main__':
    unittest.main()
