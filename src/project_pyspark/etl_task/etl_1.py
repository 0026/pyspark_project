import pyspark.sql.functions as F

def it_data(personal_and_sales_info,calls_per_area_info, save_path = "./it_data"):

    calls_per_area_info = (
        calls_per_area_info
        .select("id", "area", "calls_successful")
        .filter(F.col("area")=="IT")
        .alias("area")
    )
    personal_and_sales_info= (
            personal_and_sales_info
            .select("id", "name")
            .alias("people")
    )

    (
        calls_per_area_info
        .join(
            personal_and_sales_info,
            on = F.col("area.id") == personal_and_sales_info["id"],
            how="left"
        )
        .groupBy(
            "people.id",
            "people.name"
        )
        .agg(F.sum("calls_successful").alias("sum_calls_successful"))
        .orderBy(F.col("sum_calls_successful").desc())
        .limit(100)
        .coalesce(1)
        .write
        .option('header', 'true')
        .mode("overwrite")
        .csv(save_path)
    )

    