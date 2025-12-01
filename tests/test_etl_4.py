from chispa.dataframe_comparer import assert_df_equality
from project_pyspark.etl_task.etl_4 import top_3
import pyspark.sql.functions as F

def test_top_3(spark, tmp_path):
    save_path = str(tmp_path)

    # Input DataFrames
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

    top_3(personal_and_sales_info, calls_per_area_info, save_path=save_path)

    # Read CSV output
    actual_df = (
        spark
        .read
        .option("inferSchema" , "true")
        .option("header", "true")
        .csv(save_path)
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

