import logging
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def is_datasets_valid(sales_info: DataFrame, personal_and_sales_info: DataFrame, calls_per_area_info: DataFrame) -> bool:
    """
    Validate the integrity and consistency of three datasets.

    This function performs several validation checks across the provided datasets:

    1.Ensures all caller_id values in dataset_three (sales_info) should match exactly 1 id value in dataset_one (calls_per_area_info).

    2. Ensures ``calls_successful`` is never greater than ``calls_made`` in dataset_one (calls_per_area_info).

    3. **Address format check**  
       Ensures the ``address`` column in ``personal_and_sales_info`` follows the
       expected format: ``"<street>, <house_number>, <zip_code>"``.  
       Individual components are validated using regular expressions:
       
       - ``street_name``: ``^[A-Za-z0-9 ]+$``  
       - ``house_number``: ``^[0-9]+$``  
       - ``zip_code``: ``^[0-9]{4} [A-Z]{2}$``

    If any check fails, a warning is logged and the final return value becomes ``False``.

    :param sales_info: Dataset containing sales information. Must include ``id`` and ``caller_id``.
    :type sales_info: pyspark.sql.DataFrame

    :param personal_and_sales_info: Dataset containing personal information, with an ``address`` column
        formatted as ``"street, house_number, zip_code"``.
    :type personal_and_sales_info: pyspark.sql.DataFrame

    :param calls_per_area_info: Dataset containing call statistics per caller, must include:
        ``id``, ``calls_made``, ``calls_successful``.
    :type calls_per_area_info: pyspark.sql.DataFrame

    :return: ``True`` if all validation checks pass, otherwise ``False``.
    :rtype: bool
    """
    is_valid = True    

    # 1 check
    joined = (
        sales_info
        .withColumnRenamed("id", "id_si")
        .join(
            calls_per_area_info, 
            on = F.col("caller_id") == calls_per_area_info["id"], 
            how = "left"
        )
    )
    unmatched_rows = joined.filter(F.col("id").isNull())

    if sales_info.count() != joined.count(): 
        is_valid = False
        logging.warning(f"There are a duplicatctes in dataset_one (calls_per_area_info)")

    if unmatched_rows.count()!=0:
        is_valid = False
        logging.warning(f"The dataset three (sales_info) has unmached rows with dataset_one (calls_per_area_info).")

    # 2 check
    if calls_per_area_info.filter(F.col("calls_made")<F.col("calls_successful")).count() > 0:
        is_valid = False
        logging.warning(f"The dataset dataset_one (calls_per_area_info) has incorrect values `calls_successful` > `calls_made`.")
    
    # 3 check

    df_parts = (
        personal_and_sales_info
        .withColumn("parts", F.split("address", ","))
        .select("parts")
    )

    df_split = (
        df_parts
        .withColumn("street_name", F.trim(F.col("parts")[0]))
        .withColumn("house_number", F.trim(F.col("parts")[1]))
        .withColumn("zip_code", F.trim(F.col("parts")[2]))
    )

    street_check = r"^[A-Za-z0-9 ]+$"
    house_number_check = r"^[0-9]+$"
    zip_code_check = r"^[0-9]{4} [A-Z]{2}$"
    
    df_checked = (
        df_split
        .withColumn("valid_street_name", F.col("street_name").rlike(street_check))
        .withColumn("valid_house_number", F.col("house_number").rlike(house_number_check))
        .withColumn("valid_zip_code", F.col("zip_code").rlike(zip_code_check))
    )

    if df_checked.filter(~F.col("valid_street_name")).count() > 0:
        is_valid = False
        logging.warning(f"The dataset dataset_two (personal_and_sales_info) has incorrect values street_name in column `address`.")
    if df_checked.filter(~F.col("valid_house_number")).count() > 0:
        is_valid = False
        logging.warning(f"The dataset dataset_two (personal_and_sales_info) has incorrect values house_number in column `address`.")
    if df_checked.filter(~F.col("valid_zip_code")).count() > 0:
        is_valid = False
        logging.warning(f"The dataset dataset_two (personal_and_sales_info) has incorrect values zip_code in column `address`.")

    return is_valid    
