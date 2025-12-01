from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def top_3(personal_and_sales_info:DataFrame, calls_per_area_info:DataFrame, save_path = "./top_3"):
    """
    Identify top-performing salespeople per area based on successful call ratio
    and sales amount, and save the results as a CSV file.

    This function performs the following steps:

    1. **Select relevant columns**  
       Extracts ``id``, ``name``, and ``sales_amount`` from ``personal_and_sales_info`` 
       and aliases it as ``people_sales``. Aliases ``calls_per_area_info`` as ``calls_info``.

    2. **Join datasets**  
       Performs a left join on ``id`` between call information and personal sales data.

    3. **Filter by performance**  
       Keeps only rows where the successful call ratio 
       (``calls_successful / calls_made``) is greater than 0.75.

    4. **Rank within each area**  
       Uses a window function partitioned by ``area`` and ordered by descending ``sales_amount`` 
       to rank salespeople.

    5. **Select top 3 performers per area**  
       Filters rows where ``rank < 4`` to get the top 3.

    6. **Write output**  
       Saves ``name``, ``area``, and ``sales_amount`` as a single CSV file with a header.

    :param personal_and_sales_info: Dataset containing personal information and sales amount, 
        must include columns ``id``, ``name``, ``sales_amount``.
    :type personal_and_sales_info: pyspark.sql.DataFrame

    :param calls_per_area_info: Dataset containing call information per person, 
        must include columns ``id``, ``area``, ``calls_made``, ``calls_successful``.
    :type calls_per_area_info: pyspark.sql.DataFrame

    :param save_path: Output directory path for the resulting CSV file.
    :type save_path: str

    :return: None. The resulting CSV file is written to disk.
    :rtype: None
    """
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