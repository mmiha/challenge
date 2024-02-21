import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType

from challenge import flatten, get_flattened_columns


@pytest.fixture
def spark():
    yield SparkSession.builder.appName("tests").getOrCreate()


@pytest.fixture
def single_level_nested_schema():
    yield StructType(
        [
            StructField("field1", IntegerType()),
            StructField(
                "field2",
                StructType(
                    [
                        StructField("subfield1", IntegerType()),
                        StructField("subfield2", IntegerType()),
                    ]
                ),
            ),
        ]
    )


def test_flatten_schema_no_flattening_needed(spark):
    """
    Tests that if the schema does not contain any complex data types
    it is returned as is.
    """
    original_schema = StructType(
        [
            StructField("field1", IntegerType()),
            StructField("field2", IntegerType()),
        ]
    )

    returned_references = get_flattened_columns(original_schema)

    assert str(returned_references[0]) == "Column<'field1'>"
    assert str(returned_references[1]) == "Column<'field2'>"


def test_flatten_schema_single_level_nesting(spark, single_level_nested_schema):
    """
    Tests that a single-level of nesting is flattend correctly.
    """
    original_schema = single_level_nested_schema

    returned_references = get_flattened_columns(original_schema)

    assert str(returned_references[0]) == "Column<'field1'>"
    assert (
        str(returned_references[1]) == "Column<'field2.subfield1 AS field2__subfield1'>"
    )
    assert (
        str(returned_references[2]) == "Column<'field2.subfield2 AS field2__subfield2'>"
    )


def test_flatten_schema_double_level_nesting(spark):
    """
    Tests that a single-level of nesting is flattend correctly.
    """
    original_schema = StructType(
        [
            StructField("field1", IntegerType()),
            StructField(
                "field2",
                StructType(
                    [
                        StructField(
                            "subfield1",
                            StructType(
                                [
                                    StructField("subsubfield1", IntegerType()),
                                ]
                            ),
                        ),
                        StructField("subfield2", IntegerType()),
                    ]
                ),
            ),
        ]
    )

    returned_references = get_flattened_columns(original_schema)

    assert str(returned_references[0]) == "Column<'field1'>"
    assert (
        str(returned_references[1])
        == "Column<'field2.subfield1.subsubfield1 AS field2__subfield1__subsubfield1'>"
    )
    assert (
        str(returned_references[2]) == "Column<'field2.subfield2 AS field2__subfield2'>"
    )


def test_flatten_dataset(spark, single_level_nested_schema):
    """
    Tests that a dataset is flattened correctly.
    """

    data = [
        (1, (10, 11)),
        (
            2,
            (
                20,
                21,
            ),
        ),
    ]

    df = spark.createDataFrame(data, single_level_nested_schema)

    flattened_df = flatten(df)

    assert flattened_df.columns == ["field1", "field2__subfield1", "field2__subfield2"]

    assert all(isinstance(field.dataType, IntegerType) for field in flattened_df.schema)
