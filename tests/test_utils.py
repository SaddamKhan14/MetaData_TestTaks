import sys
import pytest

from MetaData_TestTaks.utils.project_utils import MetaDataUtils
from pyspark.sql.types import DateType, IntegerType, FloatType, StructField, StructType, StringType, TimestampType


def setup_module(module):
     print("-----------------Calling Set Up Module------------------")

@pytest.mark.skipif(sys.version_info < (3, 6),reason="Python version too obsolete")
def test_read_schema():

    schema_input_1 = MetaDataUtils.read_schema("timestamp TimestampType(),temp FloatType(),temp_max FloatType(),temp_min FloatType(),latitude FloatType(),longitude FloatType(),city StringType()")
    schema_input_2 = MetaDataUtils.read_schema("timestamp TimestampType(),temp StringType(),temp_max StringType(),temp_min StringType(),latitude IntegerType(),longitude IntegerType(),city DateType()")

    schema_output_1 = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("temp", FloatType(), True),
        StructField("temp_max", FloatType(), True),
        StructField("temp_min", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("city", StringType(), True)
    ])
    schema_output_2 = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("temp", StringType(), True),
        StructField("temp_max", StringType(), True),
        StructField("temp_min", StringType(), True),
        StructField("latitude", IntegerType(), True),
        StructField("longitude", IntegerType(), True),
        StructField("city", DateType(), True)
    ])

    assert schema_input_1 == schema_output_1
    assert type(schema_input_1) == type(schema_output_1)

    assert schema_input_2 == schema_output_2
    assert type(schema_input_2) == type(schema_output_2)

    assert schema_input_1 != schema_output_2
    assert type(schema_input_1) == type(schema_output_2)


def teardown_module(module):
     print("-----------------Calling Tear DownModule------------------")
