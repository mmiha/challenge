"""
Module for processing event logs of advertisements and related user tracking 
events to find aggregate quantities based on this data.
"""

from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType


def get_flattened_columns(schema: StructType, parent_name: str = "") -> list[Column]:
    """
    Returns a list of column expressions for selection such that nested fields are flatted
    and the names of their parent fields are prefixed using the separator `__`.

    :param schema: the input (nested) schema.
    :param parent_name: the name of the parent field of `schema`, when called
                        recursively. Defaults to empty string.

    :return: a list of column expressions
    """
    cols = []

    for field in schema.fields:
        full_field_name = (
            ".".join([parent_name, field.name]) if parent_name else field.name
        )

        if isinstance(field.dataType, StructType):
            cols += get_flattened_columns(field.dataType, parent_name=full_field_name)
        else:
            col_ref = col(full_field_name)

            if "." in full_field_name:
                col_ref = col_ref.alias(full_field_name.replace(".", "__"))

            cols.append(col_ref)

    return cols


def flatten(nested_df: DataFrame) -> DataFrame:
    """
    Flattens a dataframe with nested fields.

    :param nested_df: the input (nested) dataframe.

    :return: a flattened form of the `nested_df`.
    """
    return nested_df.select(get_flattened_columns(nested_df.schema))
