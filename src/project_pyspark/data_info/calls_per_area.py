from pyspark.sql.types import StructType, StructField, StringType, IntegerType

calls_per_area_info = {
    'name' : "calls_per_area_info",
    "description": "Information about the area of expertise of an employee and the number of calls that were made and also calls that resulted in a sale.",
    'schema' : StructType([
        StructField('id', IntegerType(), True),
        StructField('area', StringType(), True),
        StructField('calls_made', IntegerType(), True),
        StructField('calls_successful', IntegerType(), True)
    ]),
    'distinct_columns' : ["id"],
    'not_null_columns' : ["id"],
    "read_options":{
        "header":"true"
    },
    'path' : "/mnt/d/projekt/project_pyspark/resources/dataset_one.csv",
    'expected_number_of_rows' : 1000
}

