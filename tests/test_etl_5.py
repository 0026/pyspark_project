from project_pyspark.etl_task.etl_5 import top_3_most_sold_products_per_department_in_the_netherlands
from chispa.dataframe_comparer import assert_df_equality
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def test_top_3_most_sold_products_per_department_in_the_netherlands(spark: SparkSession, temp_dir:str):
    """
    Test the `top_3_most_sold_products_per_department_in_the_netherlands` function to ensure
    it correctly identifies the top 3 most sold products in the Netherlands.

    Scenario:
    - Products in the Netherlands:
        - Product A: 10 units
        - Product B: 5 units
        - Product C: 15 units
        - Product E: 8 units
    - Product D is in Germany and should be ignored.

    Steps:
    1. Create a test DataFrame `sales_data` with products, quantities, and countries.
    2. Call `top_3_most_sold_products_per_department_in_the_netherlands` with the test data 
       and a temporary CSV output path.
    3. Read back the CSV written by the function into a DataFrame.
    4. Compare the actual results with the expected top 3 products in descending order of quantity.

    Expected outcome:
    - Product C (15 units)
    - Product A (10 units)
    - Product E (8 units)

    :param spark: SparkSession fixture used to create test DataFrames.
    :type spark: pyspark.sql.SparkSession

    :param temp_dir: Temporary directory fixture for saving CSV output.
    :type temp_dir: str

    :return: None. The test passes if the actual output matches the expected DataFrame.
    """
    data = [
        ("Product A", 10, "Netherlands"),
        ("Product B", 5, "Netherlands"),
        ("Product C", 15, "Netherlands"),
        ("Product D", 20, "Germany"),  # should be ignored
        ("Product E", 8, "Netherlands"),
    ]
    columns = ["product_sold", "quantity", "country"]
    sales_data=  spark.createDataFrame(data, columns)

    top_3_most_sold_products_per_department_in_the_netherlands(sales_data, save_path=temp_dir)
    
    actual_df = (
        spark
        .read
        .option("inferSchema" , "true")
        .option("header", "true")
        .csv(temp_dir)
    )

    expected_data = [
        ("Product C", 15),
        ("Product A", 10),
        ("Product E", 8),
    ]
    expected_columns = ["product_sold", "amount"]
    expected_df = spark.createDataFrame(expected_data, expected_columns).withColumn("amount", F.col("amount").cast("int"))

    assert_df_equality(actual_df, expected_df, ignore_row_order=True)