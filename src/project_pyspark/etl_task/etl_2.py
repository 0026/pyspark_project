import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import logging

def marketing_address_information(personal_and_sales_info:DataFrame, calls_per_area_info: DataFrame, save_path = "./marketing_address_info"):
    """
    Extract postcode information for individuals who belong to the Marketing area
    and save the results as a CSV file.

    This function performs the following steps:

    1. Filters ``calls_per_area_info`` to retain only rows where ``area == "Marketing"``.
    2. Selects ``id`` and ``address`` from ``personal_and_sales_info``.
    3. Joins Marketing-area records with personal information on ``id``.
    4. Extracts a Dutch-style postcode from the ``address`` column using the pattern::

           (\d{4}\s?[A-Za-z]{2})

       This matches:
       - 4 digits  
       - Optional whitespace  
       - 2 letters  

    5. Saves the resulting dataset—which contains ``address`` and the extracted
       ``post_code`` field—to the specified directory as a single CSV file with a header.

    :param personal_and_sales_info: Dataset containing personal information including
        the ``address`` column.
    :type personal_and_sales_info: pyspark.sql.DataFrame

    :param calls_per_area_info: Dataset containing area information (e.g., Marketing, IT),
        must include ``id`` and ``area`` columns.
    :type calls_per_area_info: pyspark.sql.DataFrame

    :param save_path: Output directory to save the resulting CSV file.
        Defaults to ``"./marketing_address_info"``.
    :type save_path: str, optional

    :return: None. The final dataset is written to disk.
    :rtype: None
    """
    logging.info("start - marketing_address_information")
    calls_per_area_info = (
        calls_per_area_info
        .select("id", "area")
        .filter(F.col("area")=="Marketing")
        .alias("area")
    )
    personal_and_sales_info= (
        personal_and_sales_info
        .select("id", "address")
        .alias("people")
    )
    
    # \d -matches a digit (equivalent to [0-9])
    # \s - matches any whitespace character
    # ? - matches the previous token between zero and one times
    postcode_pattern = r"(\d{4}\s?[A-Za-z]{2})"

    (
        calls_per_area_info
        .join(
            personal_and_sales_info,
            on = F.col("area.id") == personal_and_sales_info["id"],
            how="left"
        )
        .select(
            F.col("address"),
            F.regexp_extract(F.col("address"), postcode_pattern, 1).alias("post_code")

        )
        .coalesce(1)
        .write
        .option('header', 'true')
        .mode("overwrite")
        .csv(save_path)
    )
    logging.info("finish - marketing_address_information")