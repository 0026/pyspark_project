from project_pyspark.etl_task.etl_2 import marketing_address_information
from chispa import assert_df_equality

def test_marketing_address_information(spark, temp_dir):

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

