"""Classes and methods to help load, transform, and validate column_mappings.
"""

import logging
from typing import Dict
import great_expectations as ge
import pandas as pd
from tableschema import Schema

from etl.helpers import common, drive, table_schema

INTERNAL_COLUMN_NAME_COLUMN_NAME = "Internal Column Name"
MI_FIELD_NAME_COLUMN_NAME = "Mission Impact Field Name"
COLUMN_NAMES = [INTERNAL_COLUMN_NAME_COLUMN_NAME, MI_FIELD_NAME_COLUMN_NAME]

ColumnMapping = Dict[str, str]


class ColumnMappingValidator:
    """Validates a column mapping file."""

    def __init__(self, table_schema: Schema, row_format: bool = True):
        self.table_schema: Schema = table_schema
        self.row_format: bool = row_format

    def _get_expectations(self, column_mapping: pd.DataFrame) -> ge.dataset.Dataset:
        """
        Returns great_expectations object for a pd.DataFrame with expectations attached.

        If not all expectations have been satisfied, this function may fail early for
        readability of the failed expectation(s).

        Expectations:
        - Has columns matching exactly to COLUMN_NAMES
        - Does not have any repeated local column names
        - All supposed GII columns are valid ones
        """
        column_mapping_ge = ge.from_pandas(column_mapping)
        column_mapping_ge.set_default_expectation_argument("result_format", "COMPLETE")

        # Shape
        shape_expectation = column_mapping_ge.expect_table_columns_to_match_ordered_list(
            COLUMN_NAMES
        )

        # If the shape isn't correct then the following expectations will raise exceptions
        if not shape_expectation["success"]:
            return column_mapping_ge

        # Check that there are no repeats of an internal column name
        column_mapping_ge.expect_column_values_to_be_unique(
            INTERNAL_COLUMN_NAME_COLUMN_NAME
        )

        # Check that all Mission Impact field names are valid
        valid_field_names = table_schema.get_valid_field_names(
            self.table_schema, self.row_format
        )
        column_mapping_ge.expect_column_values_to_be_in_set(
            MI_FIELD_NAME_COLUMN_NAME, valid_field_names
        )

        return column_mapping_ge

    def validate(self, column_mapping: pd.DataFrame) -> Dict:
        """Returns map of failures. If map is empty, the column mappings are valid."""
        return common.extract_failures_from_ge_result(
            self._get_expectations(column_mapping).validate()
        )


class ColumnMappingLoader:
    """Loads column mappings from a csv file."""

    @staticmethod
    def convert_column_mapping_dataframe_to_dict(
        column_mapping_df: pd.DataFrame,
    ) -> ColumnMapping:
        return column_mapping_df.set_index(INTERNAL_COLUMN_NAME_COLUMN_NAME).to_dict()[
            MI_FIELD_NAME_COLUMN_NAME
        ]

    @staticmethod
    def load_column_mappings_local(column_mapping_filename: str) -> ColumnMapping:
        return pd.read_csv(column_mapping_filename)

    @staticmethod
    def load_column_mappings_from_drive(
        column_mapping_spreadsheet_id: str, drive_credentials
    ):
        service = drive.get_google_sheets_service(drive_credentials)
        return drive.load_sheet_as_dataframe(
            service, column_mapping_spreadsheet_id, "A:B"
        )


def airflow_load_column_mapping(credentials, spreadsheet_id: str, range_name, **kwargs):
    if not spreadsheet_id or spreadsheet_id == "":
        return None

    service = drive.get_google_sheets_service(credentials)
    column_mapping_df = drive.load_sheet_as_dataframe(
        service, spreadsheet_id, range_name
    )

    return column_mapping_df
