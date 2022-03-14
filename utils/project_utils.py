import traceback

from pyspark.sql.types import DateType, IntegerType, FloatType, StructType, StringType, TimestampType

class MetaDataUtils:
    """
        MetaDataUtils class used to represent an utility functions that will
        be used for processing dataframes while running etl_jobs.py
    """

    def read_schema(
            schema_string: str
    ) -> StructType:
        """
        Explanation: This function takes input schema as of string type then
                    parses and converts input schema string to its equivalent spark schema
        :param str schema_string: user input schema string for files in input folder
        :returns StructType return_schema: parsed equivalent spark schema
        """
        d_types = {
            "StringType()": StringType(),
            "IntegerType()": IntegerType(),
            "FloatType()": FloatType(),
            "DateType()": DateType(),
            "TimestampType()": TimestampType()
        }
        try:
            split_schema_string = schema_string.split(",")
            return_schema = StructType()
            for val in split_schema_string:
                a_list = val.split(" ")
                return_schema.add(a_list[0], d_types[a_list[1]], True)
        except Exception as err:
            print("ERROR occured while parsing input schema")
            print(err)
            traceback.print_exception()
        return return_schema