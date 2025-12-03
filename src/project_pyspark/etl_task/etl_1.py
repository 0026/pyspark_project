import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from common.decorator import log_decorator

@log_decorator("it_data")
def it_data(
        personal_and_sales_info:DataFrame,
        calls_per_area_info:DataFrame, 
        save_path:str = "./it_data"
    ):
    """
    Generate an aggregated report of IT-area call performance and save it as a CSV file.

    This function filters ``calls_per_area_info`` to retain only records where 
    ``area == "IT"``, joins them with personal information from 
    ``personal_and_sales_info`` on the ``id`` column, aggregates successful 
    calls per person, and outputs the top 100 performers.

    The resulting dataset includes:

    - ``id``: Person ID  
    - ``name``: Person name  
    - ``sum_calls_successful``: Total number of successful calls in the IT area  

    The final dataset is:

    - Sorted in descending order by ``sum_calls_successful``  
    - Limited to the top 100 rows  
    - Written as a single CSV file (``coalesce(1)``)  
    - Saved with a header row  

    :param personal_and_sales_info: Dataset containing personal information, must include
        ``id`` and ``name`` columns.
    :type personal_and_sales_info: pyspark.sql.DataFrame

    :param calls_per_area_info: Dataset containing call statistics per person, must include
        ``id``, ``area``, and ``calls_successful`` columns.
    :type calls_per_area_info: pyspark.sql.DataFrame

    :param save_path: Destination directory for the resulting CSV output.
                      Defaults to ``"./it_data"``.
    :type save_path: str, optional

    :return: None. The result is written to disk as CSV.
    :rtype: None
    """
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

    