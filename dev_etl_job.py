
class Log4j(object):
    """Wrapper class for Log4j JVM object.

    :param spark: SparkSession object.
    """

    def __init__(self, spark):
        # get spark app details with which to prefix all messages
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
        """Log an error.

        :param: Error message to write to log
        :return: None
        """
        self.logger.error(message)
        return None

    def warn(self, message):
        """Log a warning.

        :param: Warning message to write to log
        :return: None
        """
        self.logger.warn(message)
        return None

    def info(self, message):
        """Log information.

        :param: Information message to write to log
        :return: None
        """
        self.logger.info(message)
        return None
"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import json
import unittest
from os import environ, listdir, path

from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

# from dependencies.spark import start_spark
from etl_job import transform_data

# from dependencies import logging


def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}):

    if True:
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .master(master)
            .appName(app_name))

        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

        # add other config params
        for key, val in spark_config.items():
            spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_logger = Log4j(spark_sess)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    return spark_sess, spark_logger, config_dict


config = json.loads("""{"steps_per_floor": 21}""")
spark, *_ = start_spark()
test_data_path = '/home/u18/pyspark-debug-test/tests/test_data/'


# assemble
input_data = (
    spark
    .read
    .parquet(test_data_path + 'employees'))

data_transformed = transform_data(input_data, 21)

expected_data = (
    spark
    .read
    .parquet(test_data_path + 'employees_report'))

expected_cols = len(expected_data.columns)
expected_rows = expected_data.count()
expected_avg_steps = (
    expected_data
    .agg(mean('steps_to_desk').alias('avg_steps_to_desk'))
    .collect()[0]
    ['avg_steps_to_desk'])
# act




cols = len(expected_data.columns)
rows = expected_data.count()
avg_steps = (
    expected_data
    .agg(mean('steps_to_desk').alias('avg_steps_to_desk'))
    .collect()[0]
    ['avg_steps_to_desk'])
input_data.show(5)
data_transformed.show(5)

# assert
# assertEqual(expected_cols, cols)
# assertEqual(expected_rows, rows)
# assertEqual(expected_avg_steps, avg_steps)
# assertTrue([col in expected_data.columns
#                     for col in data_transformed.columns])

