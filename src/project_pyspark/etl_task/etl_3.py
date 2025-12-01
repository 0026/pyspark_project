import pyspark.sql.functions as F

def department_breakdown(personal_and_sales_info, calls_per_area_info, save_path="./department_breakdown"):
    personal_and_sales_info = (
        personal_and_sales_info
        .select(
            "id", "sales_amount"
        )
        .alias("people_sales")
    )

    calls_per_area_info= (
        calls_per_area_info
        .alias("calls_info")
    )
    
    (
        calls_per_area_info
        .join(
            personal_and_sales_info,
            on = F.col("calls_info.id") == personal_and_sales_info["id"],
            how = "left"
        )
        .groupBy("area")
        .agg(
            F.sum("people_sales.sales_amount").alias("sum_sales"),
            F.sum("calls_info.calls_made").alias("sum_calls_made"),
            F.sum("calls_info.calls_successful").alias("sum_calls_successful"),
        )
        .withColumn("successful_ratio", F.col("sum_calls_successful")/F.col("sum_calls_made"))
        .select(
            "area",
            "sum_sales",
            "successful_ratio",
        )
        .coalesce(1)
        .write
        .option('header', 'true')
        .mode("overwrite")
        .csv(save_path)
    )
    