from project_pyspark.etl_task.etl_3 import department_breakdown
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from chispa import assert_df_equality
from pyspark.sql import SparkSession

def test_department_breakdown(spark: SparkSession, temp_dir:str):
    """
    Test the `department_breakdown` function to ensure it correctly aggregates sales
    and call success metrics per department.

    Scenario:
    - Input DataFrames:
        - `personal_and_sales_info`: contains `id` and `sales_amount`.
        - `calls_per_area_info`: contains `id`, `area`, `calls_made`, and `calls_successful`.
    - Aggregation logic:
        - Sum `sales_amount` per department.
        - Sum `calls_made` and `calls_successful` per department.
        - Compute `successful_ratio` = total successful calls / total calls made.

    Steps:
    1. Create test DataFrames for `personal_and_sales_info` and `calls_per_area_info`.
    2. Call `department_breakdown` with the test data and a temporary CSV output path.
    3. Read the CSV output into a DataFrame.
    4. Compare the actual output with the expected aggregated results using `assert_df_equality`.

    Expected outcome:
    - Marketing department:
        - `sum_sales` = 100 + 300 = 400
        - `successful_ratio` = (5 + 9) / (10 + 12) ≈ 0.636
    - Sales department:
        - `sum_sales` = 200 + 400 = 600
        - `successful_ratio` = (6 + 4) / (8 + 4) ≈ 0.833
    - The DataFrame should match the expected schema with columns: `area`, `sum_sales`, `successful_ratio`.

    :param spark: SparkSession fixture used to create test DataFrames.
    :type spark: pyspark.sql.SparkSession

    :param temp_dir: Temporary directory fixture for saving CSV output.
    :type temp_dir: str

    :return: None. The test passes if the actual output matches the expected DataFrame.
    """
    personal_and_sales_info = spark.createDataFrame([
        (1, 100),
        (2, 200),
        (3, 300),
        (4, 400)
    ], ["id", "sales_amount"])

    calls_per_area_info = spark.createDataFrame([
        (1, "Marketing", 10, 5),
        (2, "Sales", 8, 6),
        (3, "Marketing", 12, 9),
        (4, "Sales", 4, 4)
    ], ["id", "area", "calls_made", "calls_successful"])

    department_breakdown(personal_and_sales_info, calls_per_area_info, save_path=temp_dir)

    result_df = (
        spark
        .read
        .option("header", "true")
        .option("inferSchema" , "true")
        .csv(temp_dir)
    )

    schema = StructType([
        StructField("area", StringType(), True),
        StructField("sum_sales", IntegerType(), True),
        StructField("successful_ratio", DoubleType(), True),
    ])
    
    expected_df = spark.createDataFrame([
            ("Marketing", 100 + 300, (5 + 9)/(10 + 12)),
            ("Sales", 200 + 400, (6 + 4)/(8 + 4))
        ],
            schema=schema
        )

    assert_df_equality(result_df, expected_df, ignore_row_order=True)