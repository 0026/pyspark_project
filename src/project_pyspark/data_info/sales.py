from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

sales_info = {
    'name' : "sales_info",
    "description": "Data about the sales made, which company the call was made to where the company is located, the product and quantity sold. The field \'caller_id\' matches the ids of the calls_per_area_info and personal_and_sales_info datasets.",
    'schema' : StructType([
        StructField('id', IntegerType(), True),
        StructField('caller_id', IntegerType(), True),
        StructField('company', StringType(), True),
        StructField('recipient', StringType(), True),
        StructField('age', IntegerType(), True),
        StructField('country', StringType(), True),
        StructField('product_sold', StringType(), True),
        StructField('quantity', IntegerType(), True),
    ]),
    'distinct_columns' : ["id"],
    'not_null_columns' : ["id"],
    "read_options":{
        "header":"true"
    },
    'path' : "/mnt/d/projekt/project_pyspark/resources/dataset_three.csv",
    'expected_number_of_rows' : 10000
}
