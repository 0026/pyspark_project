import logging
import  pyspark.sql.functions as F
from pyspark.sql.types import NumericType

class Dataset:
    def __init__(self, name, read_options, schema, path, expected_number_of_rows, description ="", not_null_columns=[], distinct_columns=[]):
        self.read_options = read_options
        self.schema = schema
        self.not_null_columns = not_null_columns
        self.distinct_columns = distinct_columns
        self.name= name
        self.path = path
        self.expected_number_of_rows = expected_number_of_rows
        self.description = description

    def read_data(self, spark):
        self.dataframe = (
            spark
            .read
            .options(
                **self.read_options
            )
            .schema(self.schema)
            .csv(self.path)
        )
    
    def check_data_quality(self):
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
        


