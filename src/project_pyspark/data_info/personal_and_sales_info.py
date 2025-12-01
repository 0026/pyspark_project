from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

personal_and_sales_info = {
    'name' : "personal_and_sales_info",
    "description": "personal and sales information, like \'name\', \'address\' and \'sales_amount\'",
    'schema' : StructType([
        StructField('id', IntegerType(), True),
        StructField('name', StringType(), True),
        StructField('address', StringType(), True),
        StructField('sales_amount', FloatType(), True)
    ]),
    'distinct_columns' : ["id"],
    'not_null_columns' : ["id"],
    "read_options":{
        "header":"true"
    },
    'path' : "./resources/",
    'expected_filename' :  "dataset_two.csv",
    'expected_number_of_rows' : 1000
}

