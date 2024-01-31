from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_extract, when, expr
from pyspark.sql import functions as F
from typing import Optional
from .log_utils import logger
from config.spark_config import read_file_with_dynamic_partitions
from pyspark.sql.types import StructType
from datetime import datetime
import os


class SparkProcessor:
    """
    A class to initialize SparkSession.
    """

    def __init__(self, app_name: str) -> None:
        """
        Initializes SparkSession.

        Parameters:
        - app_name (str): Name of the Spark application.
        """
        self.spark = self.initialize_spark(app_name)

    def initialize_spark(self, app_name: str) -> SparkSession:
        """
        Initializes SparkSession.

        Parameters:
        - app_name (str): Name of the Spark application.

        Returns:
        - SparkSession: Initialized SparkSession object.
        """
        try:
            spark = SparkSession.builder.appName(app_name).getOrCreate()
            logger.info("SparkSession initialized successfully")
            return spark
        except Exception as e:
            logger.error(f"Error initializing SparkSession: {str(e)}")
            raise Exception(
                f"Error initializing SparkSession: {str(e)}\nPlease make sure Spark is installed and configured properly"
            )


class DataProcessor:
    """
    A class to perform data processing tasks using Spark DataFrame.
    """

    def __init__(self) -> None:
        """
        Initializes DataProcessor.
        """
        pass

    def read_json(self, spark: SparkSession, file_path: str) -> DataFrame:
        """
        Reads JSON file into a DataFrame.

        Parameters:
        - spark (SparkSession): SparkSession object.
        - file_path (str): Path to the JSON file.

        Returns:
        - DataFrame: DataFrame containing data from the JSON file.
        """
        try:
            df = spark.read.option("mode", "PERMISSIVE").json(file_path)
            n = read_file_with_dynamic_partitions(file_path)
            df = df.repartition(n)
            logger.info(f"Successfully read JSON file from path: {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error reading JSON file from path {file_path}: {str(e)}")
            raise

    def filter_beef_recipes(self, df: DataFrame) -> DataFrame:
        """
        Filters DataFrame to include only beef recipes.

        Parameters:
        - df (DataFrame): Input DataFrame.

        Returns:
        - DataFrame: Filtered DataFrame containing only beef recipes.
        """
        try:
            beef_recipes_df = df.filter(expr("lower(ingredients) like '%beef%'"))
            logger.info("Filtered beef recipes successfully")
            return beef_recipes_df
        except Exception as e:
            logger.error(f"Error filtering beef recipes: {str(e)}")
            raise

    def display_results(self, df: DataFrame, num_rows: int = 10) -> None:
        """
        Displays DataFrame contents.

        Parameters:
        - df (DataFrame): DataFrame to display.
        - num_rows (int): Number of rows to display (default is 10).
        """
        try:
            logger.info("Displaying DataFrame.....")
            df.show(num_rows, truncate=False)
        except Exception as e:
            logger.error(f"Error displaying results: {str(e)}")
            raise

    def extract_time_duration(self, df: DataFrame, column_name: str) -> DataFrame:
        """
        Extracts time duration from a column having duration in 'PT' formats.

        Parameters:
        - df (DataFrame): Input DataFrame.
        - column_name (str): Name of the column containing time information.

        Returns:
        - DataFrame: DataFrame with extracted time duration column.
        """
        try:
            numeric_value_expr = "(\d+)"
            unit_expr = "(M|H|S)"
            df = df.withColumn(
                "numeric_value", regexp_extract(col(column_name), numeric_value_expr, 1)
            ).withColumn("unit", regexp_extract(col(column_name), unit_expr, 1))
            df = df.withColumn("numeric_value", df["numeric_value"].cast("int"))
            derived_column_name = column_name + "_duration"
            df = df.withColumn(
                derived_column_name,
                when(df["unit"] == "M", df["numeric_value"] * 1).otherwise(
                    when(df["unit"] == "H", df["numeric_value"] * 60).otherwise(
                        when(df["unit"] == "S", df["numeric_value"] / 60).otherwise(0)
                    )
                ),
            )
            logger.info(
                f"Successfully extracted time duration from column '{column_name}'"
            )
            return df
        except Exception as e:
            logger.error(
                f"Error extracting time duration from column '{column_name}': {str(e)}"
            )
            raise

    def categorize_cook_time(self, df: DataFrame, column_name: str) -> DataFrame:
        """
        Categorizes cook time based on a specified column in DataFrame.

        Parameters:
        - df (DataFrame): Input DataFrame.
        - column_name (str): Name of the column containing cook time information.

        Returns:
        - DataFrame: DataFrame with a new column indicating cook time category.
        """
        try:
            easy_condition = col(column_name) < 30
            medium_condition = (col(column_name) >= 30) & (col(column_name) <= 60)
            hard_condition = col(column_name) > 60
            df = df.withColumn(
                "difficulty",
                when(easy_condition, "easy")
                .when(medium_condition, "medium")
                .when(hard_condition, "hard")
                .otherwise("unknown"),
            )
            logger.info(
                f"Successfully categorized cook time for column '{column_name}'"
            )
            return df
        except Exception as e:
            logger.error(
                f"Error categorizing cook time for column '{column_name}': {str(e)}"
            )
            raise

    def calculate_avg_cooking_time(
        self, df: DataFrame, column1: str, column2: str
    ) -> DataFrame:
        """
        Calculates average cooking time based on specified columns in DataFrame.

        Parameters:
        - df (DataFrame): Input DataFrame.
        - column1 (str): Name of the column for grouping.
        - column2 (str): Name of the column containing cooking time.

        Returns:
        - DataFrame: DataFrame with average cooking time calculated.
        """
        try:
            avg_cooking_time_df = df.groupBy(column1).agg(
                F.avg(column2).alias("avg_total_cooking_time")
            )
            logger.info("Successfully calculated average cooking time")
            return avg_cooking_time_df
        except Exception as e:
            logger.error(f"Error calculating average cooking time: {str(e)}")
            raise

    def writeToCSV(self, df: DataFrame, path: str) -> None:
       """
         Writes DataFrame to CSV file.
    
         Parameters:
         - df (DataFrame): DataFrame to write.
         - path (str): Path to write the CSV file.
       """
       try:
        # Generate current date
         current_date = datetime.now()
         year_month_day = current_date.strftime("%Y/%m/%d")

        # Extract the filename from the path
         file_name = path.split("/")[-1]

        # Construct the output directory path
         output_directory = os.path.join(path, year_month_day)

        # Ensure the output directory exists, create if not
         os.makedirs(output_directory, exist_ok=True)

        # Write DataFrame to CSV inside the dated directory
         df.write.csv(os.path.join(output_directory, file_name), mode="overwrite")
         logger.info("Successfully written to CSV")
       except Exception as e:
            logger.error(f"Error writing to CSV: {str(e)}")
            raise
