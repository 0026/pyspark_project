from project_pyspark.common.dataset import Dataset
from pyspark.sql.types import StructType, StructField, LongType, StringType
from chispa import assert_df_equality
from pyspark.sql import SparkSession



def test_dataset_reading_data(spark: SparkSession, temp_dir:str):
    """
    Scenario:
        Validate that the Dataset.read_data() method correctly loads a CSV file
        into a Spark DataFrame using the provided schema and read options.

    Purpose:
        This test ensures that:
        - A CSV dataset written to disk can be read back using the Dataset class.
        - The DataFrame loaded by Dataset.read_data() exactly matches the original
          DataFrame used to generate the test CSV.
        - Schema, read options, and path construction behave as expected.

    Given:
        - A Spark session.
        - A temporary directory into which a small test DataFrame is written as CSV.
        - A Dataset configuration pointing to that directory.
        - A CSV file written using header=true.

    When:
        - Dataset.read_data() is executed with the provided Spark session.

    Then:
        - The loaded DataFrame must match the original DataFrame (ignoring row order).
        - Row count must match the expected value.
        - No differences in schema or field values should exist.

    Notes:
        - expected_filename is set to an empty string to test the behavior when
          reading directly from a directory rather than a specific file.
        - assert_df_equality from chispa is used for DataFrame comparison.
    """

    test_data = spark.createDataFrame(
        [
            (1, "AAA"),
            (2, "BBB"),

        ],
        ["id", "text"]
    )

    (
        test_data
        .write
        .option("header","true")
        .mode("overwrite")
        .csv(temp_dir)
    )

    test_conf = {
        'name' : "my_test_data",
        "description": "Test data.",
        'schema' : StructType([
            StructField('id', LongType(), True),
            StructField('text', StringType(), True),

        ]),
        'distinct_columns' : ["id"],
        'not_null_columns' : ["id"],
        "read_options":{
            "header":"true"
        },
        'path' : temp_dir,
        'expected_filename' :  "",
        'expected_number_of_rows' : 2
    }
    dataset = Dataset(**test_conf)
    dataset.read_data(spark)

    assert_df_equality(dataset.dataframe, test_data, ignore_row_order=True)




