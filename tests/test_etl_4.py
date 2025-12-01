from chispa.dataframe_comparer import assert_df_equality
from project_pyspark.etl_task.etl_4 import top_3
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def test_top_3(spark: SparkSession, temp_dir:str):
    """
    Test the `top_3` function to ensure it correctly identifies the top 3 salespeople 
    per area based on their sales amount and a minimum success rate threshold for calls.

    Scenario:
    - Input DataFrames:
        - `personal_and_sales_info`: contains salesperson `id`, `name`, and `sales_amount`.
        - `calls_per_area_info`: contains `id`, `area`, `calls_made`, and `calls_successful`.
    - Selection criteria:
        - Only include salespeople with a call success ratio greater than 0.75.
        - Rank salespeople by `sales_amount` within each area.
        - Select top 3 per area.
    - Expected top performers per area:
        - Marketing: Alice (500), Bob (400), Charlie (300)
        - Sales: David (600), Frank (700)  

    Steps:
    1. Create test DataFrames for `personal_and_sales_info` and `calls_per_area_info`.
    2. Call `top_3` with the test data and a temporary CSV output path.
    3. Read the CSV output into a DataFrame.
    4. Compare the actual output with the expected DataFrame using `assert_df_equality`.

    Expected outcome:
    - Only salespeople meeting the success rate threshold are included.
    - The top performers per area are correctly selected and written to CSV.

    :param spark: SparkSession fixture used to create test DataFrames.
    :type spark: pyspark.sql.SparkSession

    :param temp_dir: Temporary directory fixture for saving CSV output.
    :type temp_dir: str

    :return: None. The test passes if the actual output matches the expected DataFrame.
    """
    personal_and_sales_info = spark.createDataFrame([
        (1, "Alice", 500),
        (2, "Bob", 400),
        (3, "Charlie", 300),
        (4, "David", 600),
        (5, "Eve", 450),
        (6, "Frank", 700),
        (7, "Tom", 0)
    ], ["id", "name", "sales_amount"])

    calls_per_area_info = spark.createDataFrame([
        (1, "Marketing", 10, 8),  # 0.8 success
        (2, "Marketing", 10, 10), # 1.0 success
        (3, "Marketing", 10, 9),  # 0.9 success
        (7, "Marketing", 10, 0),  # 0.0 success, filtered out
        (4, "Sales", 10, 10),     # 1.0 success
        (5, "Sales", 10, 6),      # 0.6 success, filtered out
        (6, "Sales", 10, 9)       # 0.9 success
    ], ["id", "area", "calls_made", "calls_successful"])

    top_3(personal_and_sales_info, calls_per_area_info, save_path=temp_dir)

    actual_df = (
        spark
        .read
        .option("inferSchema" , "true")
        .option("header", "true")
        .csv(temp_dir)
    )

    expected_data = [
        ("Alice", "Marketing", 500),
        ("Bob", "Marketing", 400),
        ("Charlie", "Marketing", 300),
        ("David", "Sales", 600),
        ("Frank", "Sales", 700)
    ]
    expected_df = spark.createDataFrame(
        expected_data,
        ["name", "area", "sales_amount"]
    ).withColumn("sales_amount", F.col("sales_amount").cast("int"))

    assert_df_equality(actual_df, expected_df, ignore_row_order=True)

