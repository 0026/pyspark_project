import pyspark.sql.functions as F

def marketing_address_information(personal_and_sales_info, calls_per_area_info, save_path = "./marketing_address_info"):
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