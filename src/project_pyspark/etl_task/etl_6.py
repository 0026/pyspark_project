from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from common.decorator import log_decorator

@log_decorator("best_salesperson")
def best_salesperson(sales_info: DataFrame, personal_and_sales_info, save_path = "./best_salesperson"):
    """
    Identify the best salesperson per country based on the total number of items sold
    and save the results as a CSV file.

    This function performs the following steps:

    1. **Join datasets**  
       Joins ``sales_info`` with ``personal_and_sales_info`` on ``caller_id`` and ``id``
       to get salesperson names and country information.

    2. **Aggregate sales per salesperson**  
       Groups the data by ``name`` and ``country`` and sums the ``quantity`` 
       column to compute ``items_sold``.

    3. **Rank salespeople within each country**  
       Uses a window function partitioned by ``country`` and ordered by descending 
       ``items_sold`` to rank salespeople.

    4. **Select top salesperson per country**  
       Filters rows with ``rank == 1`` to get the best-performing salesperson 
       for each country.

    5. **Write output**  
       Saves the resulting dataset—including ``name``, ``country``, and ``items_sold``— 
       as a single CSV file with a header.

    :param sales_info: Dataset containing sales transactions, must include 
        ``caller_id`` and ``quantity`` columns.
    :type sales_info: pyspark.sql.DataFrame

    :param personal_and_sales_info: Dataset containing salesperson personal information, 
        must include ``id``, ``name``, and ``country`` columns.
    :type personal_and_sales_info: pyspark.sql.DataFrame

    :param save_path: Output directory path for the resulting CSV file.
        Defaults to ``"./best_salesperson"``.
    :type save_path: str, optional

    :return: None. The resulting CSV file is written to disk.
    :rtype: None
    """
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
        .drop("rank")
        .coalesce(1)
        .write
        .option('header', 'true')
        .mode("overwrite")
        .csv(save_path)
    )

