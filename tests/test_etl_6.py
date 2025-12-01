from chispa.dataframe_comparer import assert_df_equality
from project_pyspark.etl_task.etl_6 import best_salesperson
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def test_best_salesperson(spark: SparkSession, temp_dir:str):
    """
    Test the `best_salesperson` function to ensure it correctly identifies the top-performing
    salesperson per country based on total items sold.

    Scenario:
    - Netherlands:
        - Alice sold 10 items
        - Bob sold 5 items
    - Germany:
        - Charlie sold 8 items
        - Diana sold 7 items

    Steps:
    1. Create test DataFrames for `sales_info` and `personal_and_sales_info`.
    2. Call `best_salesperson` with the test data and a temporary CSV output path.
    3. Read back the CSV written by the function into a DataFrame.
    4. Compare the actual results with the expected top salesperson per country.

    Expected outcome:
    - Netherlands: Alice should be identified as the top salesperson.
    - Germany: Charlie should be identified as the top salesperson.

    :param spark: SparkSession fixture used to create test DataFrames.
    :type spark: pyspark.sql.SparkSession

    :param temp_dir: Temporary directory fixture for saving CSV output.
    :type temp_dir: str

    :return: None. The test passes if the actual output matches the expected DataFrame.
    """

    sales_info_data = [
        (1, 10),
        (2, 5),
        (3, 8),
        (4, 7),
    ]
    sales_info_columns = ["caller_id", "quantity"]
    sales_info = spark.createDataFrame(sales_info_data, sales_info_columns)

    personal_and_sales_info_data = [
        (1, "Alice", "Netherlands"),
        (2, "Bob", "Netherlands"),
        (3, "Charlie", "Germany"),
        (4, "Diana", "Germany"),
    ]
    personal_and_sales_info_columns = ["id", "name", "country"]
    
    personal_and_sales_info= spark.createDataFrame(personal_and_sales_info_data, personal_and_sales_info_columns)

    best_salesperson(sales_info, personal_and_sales_info, temp_dir)

    actual_df = (
        spark
        .read
        .option("inferSchema" , "true")
        .option("header", "true")
        .csv(temp_dir)
    )

    expected_data = [
        ("Alice", "Netherlands", 10),
        ("Charlie", "Germany", 8),  # 8 vs 7 -> Charlie wins
    ]
    expected_columns = ["name", "country", "items_sold"]
    expected_df = spark.createDataFrame(expected_data, expected_columns).withColumn("items_sold", F.col("items_sold").cast("int"))

    assert_df_equality(actual_df, expected_df, ignore_row_order=True)