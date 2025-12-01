import pyspark.sql.functions as F

def top_3_most_sold_products_per_department_in_the_netherlands(sales_info, save_path = "./top_3_most_sold_per_department_netherlands"):
    (
        sales_info
        .filter(F.col("country")=="Netherlands")
        .groupBy("product_sold")
        .agg(F.sum("quantity").alias("amount"))
        .orderBy(F.col("amount").desc())
        .limit(3)
        .coalesce(1)
        .write
        .option('header', 'true')
        .csv(save_path)
    )