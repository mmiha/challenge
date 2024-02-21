"""
Module for processing event logs of advertisements and related user tracking 
events to find aggregate quantities based on this data.
"""

import os

from azure.storage.blob import BlobServiceClient
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import StructType

TEMP_PATH = "/tmp/challenge-data/"


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


class AzureStorageConnector:
    """
    A class giving read and write access to
    Azure Blob Storage.
    """

    def __init__(self, conn_string: str):
        """
        Creates a BlobServiceClient.

        :param conn_string: connection string to use.
        """
        os.makedirs(TEMP_PATH, exist_ok=True)

        self.blob_service_client = BlobServiceClient.from_connection_string(conn_string)

    def get_file(self, file_name: str):

        blob_client = self.blob_service_client.get_blob_client(
            container="challenge-data", blob=file_name
        )

        with open(file=os.path.join(TEMP_PATH, file_name), mode="wb") as f:
            download_stream = blob_client.download_blob()
            f.write(download_stream.readall())

    def write_file(self, file_name: str):
        blob_client = self.blob_service_client.get_blob_client(
            container="challenge-data", blob=file_name
        )

        with open(file=os.path.join(TEMP_PATH, file_name), mode="rb") as f:
            blob_client.upload_blob(f)


def remove_underscores_from_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Cleans a string column by removing any underscores.

    :param df: the input dataframe.
    :param column_name: the name of the column to process.

    :return: The input dataframe with column `column_name` cleaned.
    """
    return df.withColumn(column_name, regexp_replace(column_name, r"_", ""))
