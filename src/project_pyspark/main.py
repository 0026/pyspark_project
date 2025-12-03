from pyspark.sql import SparkSession

from common.dataset import Dataset
from common.helper import is_datasets_valid

from etl_task.etl_1 import it_data
from etl_task.etl_2 import marketing_address_information
from etl_task.etl_3 import department_breakdown
from etl_task.etl_4 import top_3
from etl_task.etl_5 import top_3_most_sold_products_per_department_in_the_netherlands
from etl_task.etl_6 import best_salesperson

from data_info.calls_per_area import calls_per_area_info
from data_info.personal_and_sales_info import personal_and_sales_info
from data_info.sales import sales_info
import argparse
import logging

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("start app")

    spark = (
        SparkSession
        .builder
        .appName("Python Spark tasks")
        .getOrCreate()
    )

    parser=argparse.ArgumentParser()
    parser.add_argument("--sales_info_path", help="path to sales_info data")
    parser.add_argument("--personal_and_sales_info", help="path to personal_and_sales_info data")
    parser.add_argument("--calls_per_area_info", help="path to calls_per_area_info data")
    parser.add_argument("--additional_check", help="stop if additional check (y/n)")
    parser.add_argument("--common_path", help="path for all files")

    args=parser.parse_args()

    if (args.additional_check is None) | (args.common_path is None):
        logging.info("Missing required parameters: 'additional_check' and/or 'common_path'")
        return

    fail_on_quality_issue = True if args.additional_check=="y" else False

    data_config = {
        "sales_info": sales_info,
        "personal_and_sales_info": personal_and_sales_info, 
        "calls_per_area_info" : calls_per_area_info
    }

    logging.info("setting configuration")
    for key in data_config.keys():
        data_config[key]["path"] = args.common_path

    if args.sales_info_path:
        data_config["sales_info"]["path"] = args.sales_info_path
        data_config["sales_info"]["expected_filename"] = ''

    if args.personal_and_sales_info:
        data_config["personal_and_sales_info"]["path"] = args.personal_and_sales_info
        data_config["personal_and_sales_info"]["expected_filename"] = ''

    if args.calls_per_area_info:
        data_config["calls_per_area_info"]["path"] = args.sales_info_path
        data_config["calls_per_area_info"]["expected_filename"] = ''

    data = {} 
    for i in data_config.values():
        ds = Dataset(**i)
        ds.read_data(spark)
        data[ds.name] = ds

    if (not is_datasets_valid(
        data["sales_info"].dataframe , data["personal_and_sales_info"].dataframe, data["calls_per_area_info"].dataframe 
    )) & fail_on_quality_issue:
        return

    logging.info("start processing")
    it_data(data["personal_and_sales_info"].dataframe, data["calls_per_area_info"].dataframe)
    marketing_address_information(data["personal_and_sales_info"].dataframe, data["calls_per_area_info"].dataframe)
    department_breakdown(data["personal_and_sales_info"].dataframe, data["calls_per_area_info"].dataframe)
    top_3(data["personal_and_sales_info"].dataframe, data["calls_per_area_info"].dataframe)
    top_3_most_sold_products_per_department_in_the_netherlands(data["sales_info"].dataframe)
    best_salesperson(data["sales_info"].dataframe, data["personal_and_sales_info"].dataframe)
    logging.info("processing ended")

    spark.stop()

if __name__ == "__main__":
    main()
