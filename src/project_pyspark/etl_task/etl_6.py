from pyspark.sql import Window
import pyspark.sql.functions as F

def best_salesperson(sales_info, personal_and_sales_info, save_path = "./best_salesperson"):

    window =  Window.partitionBy("country").orderBy(F.desc("items_sold"))
    
    (
        sales_info
        .join(
            personal_and_sales_info,
            on = F.col("caller_id") ==personal_and_sales_info["id"],
            how='left'
        )
        .groupBy("name", "country")
        .agg(
            F.sum("quantity").alias("items_sold")
        )
        .withColumn(
            "rank", F.rank().over(window)
        )
        .filter(F.col("rank")==1)
        .coalesce(1)
        .write
        .option('header', 'true')
        .csv(save_path)
    )
