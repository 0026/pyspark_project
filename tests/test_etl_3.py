from project_pyspark.etl_task.etl_3 import department_breakdown
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from chispa import assert_df_equality

def test_department_breakdown_chispa(spark, tmp_path):

    save_path = str(tmp_path)

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

    department_breakdown(personal_and_sales_info, calls_per_area_info, save_path=str(save_path))

    result_df = (
        spark
        .read
        .option("header", "true")
        .option("inferSchema" , "true")
        .csv(save_path)
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