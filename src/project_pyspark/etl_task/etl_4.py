from pyspark.sql import Window
import pyspark.sql.functions as F

def top_3(personal_and_sales_info, calls_per_area_info, save_path = "./top_3"):
    personal_and_sales_info = (
        personal_and_sales_info
        .select(
            "id", "name","sales_amount"
        )
        .alias("people_sales")
    )


    calls_per_area_info= (
        calls_per_area_info
        .alias("calls_info")
    )

    window =  Window.partitionBy("area").orderBy(F.desc("sales_amount"))
    
    (
        calls_per_area_info
        .join(
            personal_and_sales_info,
            on = F.col("calls_info.id") == personal_and_sales_info["id"],
            how = "left"
        )
        .filter(
            F.col("calls_successful")/F.col("calls_made") > 0.75
        )
        .withColumn(
            "rank", F.rank().over(window)
        )
        .filter(F.col("rank")<4)
        .select(
            "name",
            "area",
            "sales_amount"
        )
                .coalesce(1)
        .write
        .option('header', 'true')
        .mode("overwrite")
        .csv(save_path)
    )