import sys
import os

sys.path.append("C://HD/HelloFresh/SubashK-HFtest-data-engineering/src")


def read_file_with_dynamic_partitions(file_path):
    """
    Read a file into a DataFrame and dynamically create partitions based on the file size.

    Args:
        file_path (str): Path to the file.
        desired_partition_size (int): Desired partition size in bytes.

    Returns:
        DataFrame: DataFrame with dynamically created partitions.
    """

    # Get the size of the DataFrame
    file_size = os.path.getsize(file_path)
    desired_partition_size = 100 * 1024 * 1024
    # Calculate the number of partitions based on the file size and desired partition size
    num_partitions = max(1, file_size // desired_partition_size)

    return num_partitions


def calculate_spark_resources(file_size: float, datastorage: str) -> tuple:
    """
    Calculate executor count, partition count, and executor memory for Spark processing.

    Args:
        file_size (float): The input data size in GB.
        datastorage (str): The unit of data storage, either 'GB' or 'MB' (case-insensitive).

    Returns:
        tuple: A tuple containing executor count, partition count, and executor memory.

    Example:
        calculate_spark_resources(20, 'GB')
    """
    # Check if file_size is numeric
    if not isinstance(file_size, (int, float)):
        raise ValueError("file_size must be a numeric value")

    # Convert datastorage to lowercase
    datastorage = datastorage.lower()

    # Check if datastorage is 'GB' and convert it to 'MB'
    if datastorage == 'gb':
        file_size *= 1024

    # Constants
    default_block_size = 128
    Max_CPU_cores_per_exec = 4
    constant_value = 4

    print("Considerations.....")
    print(f"Default Partition/block size in MB: {default_block_size}")
    print(f"Maximum CPU cores per executor:{Max_CPU_cores_per_exec} ")

    # Calculate partition count
    partitions_count = file_size / default_block_size

    # Calculate executor count
    executor_count = partitions_count / Max_CPU_cores_per_exec

    # Calculate executor memory
    max_executor_memory = constant_value * default_block_size * Max_CPU_cores_per_exec

    return executor_count, partitions_count, max_executor_memory

# Example usage
file_size = 1024
datastorage = 'GB'
executor_count, partitions_count, max_executor_memory = calculate_spark_resources(file_size, datastorage)
print(f"Number of partitions: {partitions_count}")
print(f"Number of cores required is equal to number of partitions: {partitions_count}")
print(f"Maximum CPU cores per executor in YARN: {5}")
print(f"Number of executors required to process {file_size} {datastorage} is: {executor_count}")
print(f"Per Executor, maximum memory is: {max_executor_memory} MB")

# Repartition the DataFrame based on the calculated number of partitions
