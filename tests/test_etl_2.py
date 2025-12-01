from project_pyspark.etl_task.etl_2 import marketing_address_information
from chispa import assert_df_equality
from pyspark.sql import SparkSession

def test_marketing_address_information(spark: SparkSession, temp_dir:str):
    """
    Test the `marketing_address_information` function to ensure it correctly extracts
    postal codes for Marketing area contacts and writes the results to a CSV.

    Scenario:
    - Input DataFrames:
        - `personal_and_sales_info`: contains `id` and `address`.
        - `calls_per_area_info`: contains `id` and `area`.
    - Selection criteria:
        - Only include contacts in the "Marketing" area.
        - Extract the postal code from the `address` column using the pattern `\\d{4}\\s?[A-Za-z]{2}`.

    Steps:
    1. Create test DataFrames for `personal_and_sales_info` and `calls_per_area_info`.
    2. Call `marketing_address_information` with the test data and a temporary CSV output path.
    3. Read the CSV output into a DataFrame.
    4. Compare the actual output with the expected DataFrame using `assert_df_equality`.

    Expected outcome:
    - Only addresses associated with the Marketing area are included.
    - The `post_code` column correctly extracts the postal code from the address.

    Example expected output:
        - ("123 Main St 1234 AB", "1234 AB")
        - ("789 Other St 9101 EF", "9101 EF")

    :param spark: SparkSession fixture used to create test DataFrames.
    :type spark: pyspark.sql.SparkSession

    :param temp_dir: Temporary directory fixture for saving CSV output.
    :type temp_dir: str

    :return: None. The test passes if the actual output matches the expected DataFrame.
    """
    personal_and_sales_info = spark.createDataFrame([
        (1, "123 Main St 1234 AB"),
        (2, "456 Side St 5678 CD"),
        (3, "789 Other St 9101 EF")
    ], ["id", "address"])

    calls_per_area_info = spark.createDataFrame([
        (1, "Marketing"),
        (2, "Sales"),
        (3, "Marketing")
    ], ["id", "area"])


    # Run the function
    marketing_address_information(personal_and_sales_info, calls_per_area_info, save_path=temp_dir)

    # Read the result
    result_df = spark.read.option("header", True).csv(temp_dir)

    expected_data = [
        ("123 Main St 1234 AB", "1234 AB"),
        ("789 Other St 9101 EF", "9101 EF")
    ]
    expected_df = spark.createDataFrame(
        expected_data,
        ["address", "post_code"]
    )

    assert_df_equality(result_df, expected_df, ignore_row_order=True)

