from project_pyspark.etl_task.etl_1 import it_data
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from chispa import assert_df_equality

def test_it_data_output_df(spark, temp_dir):
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

    save_path = temp_dir

    it_data(
        personal_and_sales_info=personal_and_sales_info,
        calls_per_area_info=calls_per_area_info,
        save_path=save_path
    )

    result_df = (
        spark
        .read
        .option('header', 'true')
        .option("mode", "overwirte")
        .option("inferSchema" , "true")
        .csv(save_path)    
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