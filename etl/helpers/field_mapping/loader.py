import os
from typing import Dict
from tableschema import Schema
import pandas as pd
from googleapiclient.discovery import Resource

from etl.helpers.field_mapping import common
from etl.helpers.field_mapping.common import FieldMapping, FieldMappings
from etl.helpers import drive


class FieldMappingLoader:
    """Helper class for loading field mappings from .csv files and Google Drive."""

    def __init__(self, table_schema: Schema):
        self.table_schema: Schema = table_schema

    @staticmethod
    def _load_field_mapping_local(
        field_name: str, config_location: str
    ) -> FieldMapping:
        """Loads field mapping from .csv file into a FieldMapping object."""
        filename: str = common.get_field_mapping_filename(field_name, config_location)
        mapping_exists: bool = os.path.exists(filename)
        if not mapping_exists:
            return None

        field_mapping_df: FieldMapping.FieldMappingDF = pd.read_csv(filename)
        return FieldMapping.from_dataframe(field_mapping_df)

    def load_field_mappings_drive(
        self, account_info, spreadsheet_id: str
    ) -> FieldMappings:
        """Loads field mappings from a Google Sheet into a FieldMappings object."""
        service: Resource = drive.get_google_sheets_service(account_info)

        results: Dict[str, pd.DataFrame] = drive.load_sheets_as_dataframes(
            service, spreadsheet_id, range="!A:C"
        )

        # Return the non-empty dataframes for fields that exist in the schema
        field_mappings: Dict[str, pd.DataFrame] = {
            field_name: FieldMapping.from_dataframe(dataframe)
            for field_name, dataframe in results.items()
            if field_name in self.table_schema.field_names and not dataframe.empty
        }

        return field_mappings

    def load_field_mappings_local(self, config_location: str) -> FieldMappings:
        """Loads field mappings from .csv files."""
        dirname: str = os.path.dirname(config_location)

        field_mappings: FieldMappings = {}
        for field_name in self.table_schema.field_names:
            field_mapping = FieldMappingLoader._load_field_mapping_local(
                field_name, dirname
            )
            if field_mapping:
                field_mappings[field_name] = field_mapping

        return field_mappings


def airflow_load_field_mappings(
    credentials: Dict, spreadsheet_id: str, load_schema_xcom_args, ti, **kwargs
) -> FieldMappings:
    """Returns field mappings from a spreadsheet."""

    if not spreadsheet_id or spreadsheet_id == "":
        return {}

    schema = ti.xcom_pull(**load_schema_xcom_args)

    field_mapping_loader: FieldMappingLoader = FieldMappingLoader(schema)
    return field_mapping_loader.load_field_mappings_drive(credentials, spreadsheet_id)
