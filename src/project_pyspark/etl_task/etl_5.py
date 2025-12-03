import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from common.decorator import log_decorator

@log_decorator("top_3_most_sold_products_per_department_in_the_netherlands")
def top_3_most_sold_products_per_department_in_the_netherlands(sales_info:DataFrame, save_path = "./top_3_most_sold_per_department_netherlands"):
    """
    Identify the top 3 most sold products in the Netherlands and save the results as a CSV file.

    This function performs the following steps:

    1. Filters ``sales_info`` to include only rows where ``country == "Netherlands"``.
    2. Groups the data by ``product_sold`` and aggregates the total quantity sold.
    3. Orders the results by descending total quantity.
    4. Selects the top 3 products.
    5. Writes the results to the specified directory as a single CSV file with a header.

    The resulting CSV contains:

    - ``product_sold``: Name or identifier of the product.
    - ``amount``: Total quantity sold in the Netherlands.

    :param sales_info: Dataset containing sales information, must include 
        ``product_sold``, ``quantity``, and ``country`` columns.
    :type sales_info: pyspark.sql.DataFrame

    :param save_path: Output directory path for the resulting CSV file.
        Defaults to ``"./top_3_most_sold_per_department_netherlands"``.
    :type save_path: str, optional

    :return: None. The resulting CSV is written to disk.
    :rtype: None
    """
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
        .mode("overwrite")
        .csv(save_path)
    )