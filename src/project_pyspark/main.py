from pyspark.sql import SparkSession

from common.dataset import Dataset

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

args=parser.parse_args()

data_config = [sales_info, personal_and_sales_info, calls_per_area_info]

if args.sales_info_path:
    sales_info["path"] = args.sales_info_path
    data_config.append(sales_info)
if args.personal_and_sales_info:
    sales_info["path"] = args.personal_and_sales_info
    data_config.append(personal_and_sales_info)
if args.sales_info_path:
    sales_info["path"] = args.calls_per_area_info
    data_config.append(calls_per_area_info)

data = {} 
for  i in data_config:
    ds = Dataset( **i)
    ds.read_data(spark)
    data[ds.name] = ds

for key in data.keys():
    print(key)
    print(data[key].path)
    data[key].dataframe.printSchema()



# it_data(data["personal_and_sales_info"].dataframe, data["calls_per_area_info"].dataframe)
# marketing_address_information(data["personal_and_sales_info"].dataframe, data["calls_per_area_info"].dataframe)
#department_breakdown(data["personal_and_sales_info"].dataframe, data["calls_per_area_info"].dataframe)
#top_3(data["personal_and_sales_info"].dataframe, data["calls_per_area_info"].dataframe)
#top_3_most_sold_products_per_department_in_the_netherlands(data["sales_info"].dataframe)
# best_salesperson(data["sales_info"].dataframe, data["personal_and_sales_info"].dataframe)
print(3)
# data_config = [sales_info, personal_and_sales_info, calls_per_area_info]
# data = {} 

# for i in data_config:
#     ds = Dataset( **i)
#     ds.read_data(spark)
#     data[ds.name] = ds
    
# for key in data.keys():
#     print(key)
#     data[key] = 

# data["sales_info"].dataframe.printSchema()
# data["sales_info"].dataframe.show()
# data["sales_info"].check_data_quality()


# df = data["sales_info"].dataframe


# df =  (
#     spark.read.csv("/mnt/d/projekt/project_pyspark/resources/dataset_three.csv")
# )

# df.show()
# df.printSchema()

# my_fun()
