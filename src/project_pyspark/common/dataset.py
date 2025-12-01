import pyspark.sql.functions as F
from pyspark.sql.types import StructType, NumericType
from pyspark.sql import SparkSession
import logging
import os


class Dataset:
    """
    Represents a dataset definition including read options, schema, path, 
    data-quality expectations, and validation rules.

    This class centralizes information about a dataset and provides methods
    to read it from disk and run basic data quality checks such as:

    - Row count validation
    - NOT NULL column validation
    - Uniqueness (distinct) column validation
    - Non-negative numeric column validation

    :param name: Name of the dataset, used in logging and identification.
    :type name: str
    :param read_options: Spark read options passed to ``DataFrameReader.options()``.
    :type read_options: dict
    :param schema: Spark schema describing the dataset structure.
    :type schema: pyspark.sql.types.StructType
    :param path: Absolute or relative path to the dataset file.
    :type path: str
    :param expected_filename: expected filename
    :type expected_filename: str
    :param expected_number_of_rows: Expected number of rows the dataset should contain.
    :type expected_number_of_rows: int
    :param description: Optional textual description of the dataset.
    :type description: str, optional
    :param not_null_columns: A list of column names that must not contain NULL values.
    :type not_null_columns: list[str]
    :param distinct_columns: A list of columns that must contain only unique values.
    :type distinct_columns: list[str]

    :ivar dataframe: Loaded Spark DataFrame after calling :meth:`read_data()`.
    :vartype dataframe: pyspark.sql.DataFrame
    """
    def __init__(
            self, 
            name:str, 
            read_options: dict, 
            schema: StructType, 
            path: str, 
            expected_filename:str, 
            expected_number_of_rows: int, 
            description:str ="", 
            not_null_columns:list[str]=[], 
            distinct_columns:list[str]=[]
        ):
        
        self.read_options = read_options
        self.schema = schema
        self.not_null_columns = not_null_columns
        self.distinct_columns = distinct_columns
        self.name= name
        self.path = path
        self.expected_filename = expected_filename
        self.expected_number_of_rows = expected_number_of_rows
        self.description = description

    def read_data(self, spark: SparkSession):
        """
        Read the dataset using the provided Spark session.

        The method loads the data into ``self.dataframe`` using:

        - ``read_options`` passed to ``.options()``
        - Predefined ``schema``
        - CSV format
        - The file path stored in ``self.path``

        :param spark: Active Spark session used for reading the dataset.
        :type spark: pyspark.sql.SparkSession

        :return: None. The resulting DataFrame is stored on ``self.dataframe``.
        :rtype: None
        """
        self.dataframe = (
            spark
            .read
            .options(
                **self.read_options
            )
            .schema(self.schema)
            .csv(os.path.join(self.path, self.expected_filename))
        )
    
    def check_data_quality(self):
        """
        Run data quality validation rules on the loaded dataset.

        The following checks are performed:

        1. **Row count check**  
           Verifies that the number of rows in the dataset matches 
           ``expected_number_of_rows``.

        2. **NOT NULL column check**  
           Ensures that each column listed in ``not_null_columns`` 
           contains no NULL values.

        3. **Distinct column check**  
           Ensures that each column listed in ``distinct_columns`` 
           contains only unique values.

        4. **Non-negative numeric check**  
           All columns with Spark numeric types must contain values >= 0.

        Any violations trigger warnings via the ``logging`` module.

        :return: None. Results are logged as warnings.
        :rtype: None
        """
        row_count = self.dataframe.count()
        if row_count != self.expected_number_of_rows:
            logging.warning(f"The dataset \"{self.name}\" has unexpected number of rows. (expected: {self.expected_number_of_rows}, has: {row_count})")
        
        for single_column in self.not_null_columns:
            if self.dataframe.filter(F.col(single_column).isNull()).count() > 0: 
                logging.warning(f"In dataset \"{self.name}\" column {single_column} contains null values (it shouldn\'t)")

        for single_column in self.distinct_columns:
            if self.dataframe.select(single_column).distinct().count() < row_count: 
                logging.warning(f"In dataset \"{self.name}\" column {single_column} contains duplicates (it shouldn\'t)")

        numeric_columns = [f.name for f in self.dataframe.schema.fields if isinstance(f.dataType, NumericType)]
        for single_column in numeric_columns:
            if self.dataframe.filter(F.col(single_column)<0).count() < 0:
                logging.warning(f"In dataset \"{self.name}\" column {single_column} contains values lower then 0 (it shouldn\'t)")
        


