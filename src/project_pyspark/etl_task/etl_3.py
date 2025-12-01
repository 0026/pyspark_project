import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import logging

def department_breakdown(personal_and_sales_info:DataFrame, calls_per_area_info:DataFrame, save_path="./department_breakdown"):
    """
    Produce a department-level breakdown of sales and call performance, and
    save the results as a CSV file.

    This function combines sales data with call performance metrics and computes
    aggregated statistics per department (``area``). The processing includes:

    1. **Selecting personal sales data**  
       Extracts only ``id`` and ``sales_amount`` from ``personal_and_sales_info``.

    2. **Joining datasets**  
       Joins call information with personal sales data on ``id`` using a left join.

    3. **Aggregating per department**  
       For each ``area`` (department), computes:
       
       - ``sum_sales``: Total sales amount  
       - ``sum_calls_made``: Total number of calls made  
       - ``sum_calls_successful``: Total number of successful calls  
       - ``successful_ratio``: Ratio of successful calls to calls made  
         (``sum_calls_successful / sum_calls_made``)

    4. **Writing output**  
       Writes the final dataset—containing ``area``, ``sum_sales``, and 
       ``successful_ratio``—to a single CSV file at the given path, with a header.

    :param personal_and_sales_info: Dataset containing personal sales data, must include:
        ``id`` and ``sales_amount``.
    :type personal_and_sales_info: pyspark.sql.DataFrame

    :param calls_per_area_info: Dataset containing call statistics, must include:
        ``id``, ``area``, ``calls_made``, ``calls_successful``.
    :type calls_per_area_info: pyspark.sql.DataFrame

    :param save_path: Output directory where the aggregated CSV will be stored.
        Defaults to ``"./department_breakdown"``.
    :type save_path: str, optional

    :return: None. The function writes the aggregated dataset to disk.
    :rtype: None
    """
    logging.info("start - department_breakdown")
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
    logging.info("finish - department_breakdown")
    