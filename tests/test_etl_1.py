from project_pyspark.etl_task.etl_1 import it_data
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from chispa import assert_df_equality
from pyspark.sql import SparkSession

def test_it_data_output_df(spark: SparkSession, temp_dir:str):
    """
    Test the `it_data` function to ensure it correctly aggregates successful calls
    for the IT department and outputs the top performers.

    Scenario:
    - Input DataFrames:
        - `personal_and_sales_info`: contains `id` and `name` of employees.
        - `calls_per_area_info`: contains `id`, `area`, and `calls_successful`.
    - Selection criteria:
        - Only include calls in the "IT" area.
        - Aggregate `calls_successful` per employee.
        - Order results by descending total calls and limit to top 100 (if applicable).

    Steps:
    1. Create test DataFrames for `personal_and_sales_info` and `calls_per_area_info`.
    2. Call `it_data` with the test data and a temporary CSV output path.
    3. Read the CSV output into a DataFrame.
    4. Compare the actual output with the expected aggregated results using `assert_df_equality`.

    Expected outcome:
    - Only IT calls are counted.
    - Aggregated `sum_calls_successful` is correct per employee.
    - The output DataFrame contains columns: `id`, `name`, `sum_calls_successful`.

    Example expected output:
        - (3, "Carol", 7)
        - (1, "Alice", 5)
        - (2, "Bob", 2)

    :param spark: SparkSession fixture used to create test DataFrames.
    :type spark: pyspark.sql.SparkSession

    :param temp_dir: Temporary directory fixture for saving CSV output.
    :type temp_dir: str

    :return: None. The test passes if the actual output matches the expected DataFrame.
    """
    personal_and_sales_info = spark.createDataFrame(
        [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carol"),
        ],
        ["id", "name"]
    )

    calls_per_area_info = spark.createDataFrame(
        [
            (1, "IT", 5),
            (1, "HR", 10),
            (2, "IT", 2),
            (3, "IT", 7),
            (3, "Sales", 3),
        ],
        ["id", "area", "calls_successful"]
    )

    it_data(
        personal_and_sales_info=personal_and_sales_info,
        calls_per_area_info=calls_per_area_info,
        save_path=temp_dir
    )

    result_df = (
        spark
        .read
        .option('header', 'true')
        .option("inferSchema" , "true")
        .csv(temp_dir)    
    )

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("sum_calls_successful", IntegerType(), True),
    ])

    expected_df = spark.createDataFrame(
        [
            (3, "Carol", 7),   # 7 IT calls
            (1, "Alice", 5),   # 5 IT calls
            (2, "Bob", 2),     # 2 IT calls
        ],
        schema=schema
    )

    assert_df_equality(result_df, expected_df, ignore_row_order=True)