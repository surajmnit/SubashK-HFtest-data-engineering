# Import necessary modules
import sys
import os

sys.path.append("C://HD/HelloFresh/SubashK-HFtest-data-engineering/src")
from utils.log_utils import logger
from config.creds import Config
from utils.spark_utils import SparkProcessor, DataProcessor
from config.spark_config import read_file_with_dynamic_partitions

# Initialize SparkProcessor
spark_processor = SparkProcessor(app_name="HelloFresh Data Prcoessing")

# Initialize DataProcessor
data_processor = DataProcessor()


try:
    # Read JSON file using SparkProcessor
    df = data_processor.read_json(spark_processor.spark, Config.INPUT_JSON_PATH)

    # Filter beef recipes using DataProcessor
    filtered_df = data_processor.filter_beef_recipes(df)

    # Extract time duration from column using DataProcessor
    duration_df = data_processor.extract_time_duration(
        filtered_df, column_name="cookTime"
    )
    duration_df = data_processor.extract_time_duration(
        duration_df, column_name="prepTime"
    )
    duration_df = duration_df.withColumn(
        "total_cooking_time",
        duration_df["cookTime_duration"] + duration_df["prepTime_duration"],
    )

    # Categorize cook time using DataProcessor
    categorized_df = data_processor.categorize_cook_time(
        duration_df, column_name="total_cooking_time"
    )

    # Calculate average cooking time using DataProcessor
    avg_cooking_time_df = data_processor.calculate_avg_cooking_time(
        categorized_df, column1="difficulty", column2="total_cooking_time"
    )
    avg_cooking_time_df = avg_cooking_time_df.select(
        "difficulty", "avg_total_cooking_time"
    )
    avg_cooking_time_df = avg_cooking_time_df.cache()

    # Display average cooking time
    data_processor.display_results(avg_cooking_time_df)
    data_processor.writeToCSV(avg_cooking_time_df, Config.OUTPUT_CSV_PATH)

except Exception as e:
    logger.error(f"An error occurred: {str(e)}")
