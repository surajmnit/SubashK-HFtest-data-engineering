import pytest
import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
import sys
import os
from pyspark.sql.functions import expr


sys.path.append("C://HD/HelloFresh/SubashK-HFtest-data-engineering/src")


from config.creds import Config
from utils.spark_utils import SparkProcessor, DataProcessor
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark_processor():
    """
    Fixture to initialize SparkProcessor for testing.
    """
    return SparkProcessor(app_name="Test")


def test_initialize_spark(spark_processor):
    """
    Test initializing SparkSession.
    """
    assert spark_processor.spark is not None


@pytest.fixture(scope="module")
def data_processor():
    """
    Fixture to initialize DataProcessor for testing.
    """
    return DataProcessor()

def test_read_json(data_processor):
    """
    Test reading JSON file.
    """
    spark = SparkSession.builder.appName("Test").getOrCreate()
    df = data_processor.read_json(spark, Config.INPUT_TEST_FILE)
    assert df is not None
    # Add more assertions as needed


def test_filter_beef_recipes(data_processor):
    """
    Test filtering beef recipes.
    """

    spark = SparkSession.builder.appName("Test").getOrCreate()
    df = data_processor.read_json(spark, Config.INPUT_TEST_FILE)

    filtered_df = data_processor.filter_beef_recipes(df)
    fg = df.filter(expr("lower(ingredients) like '%beef%'"))
    assert filtered_df.count() == fg.count()


def test_extract_time_duration(data_processor):
    """
    Test extracting time duration.
    """
    dummy_data = [
        ("PT45M",),
        ("PT20M",),
    ]
    spark = SparkSession.builder.appName("Test").getOrCreate()
    df = spark.createDataFrame(dummy_data, ["cookTime"])

    extracted_df = data_processor.extract_time_duration(df, "cookTime")
    assert "cookTime_duration" in extracted_df.columns
