import os
import csv
import logging
from tableschema import Schema, Field
from googleapiclient.discovery import Resource
from typing import Dict, List

from etl.helpers.field_mapping import common
from etl.helpers.field_mapping.common import FieldMapping, FieldMappings
from etl.helpers import drive
from etl.helpers.drive import SheetInfo
from etl.helpers import table_schema


def get_enum_options(field: Field) -> List[str]:
    enum_options = []
    if field.type == "boolean":
        enum_options = field.descriptor["trueValues"] + field.descriptor["falseValues"]
    elif field.type == "integer":
        enum_options = field.descriptor["enum_mapping"].keys()
    elif field.type == "string":
        enum_options = field.constraints["enum"]

    return enum_options


class FieldMappingWriter:
    """Helper class for writing field mappings to .csv files and Google Sheets."""

    def __init__(self, table_schema: Schema):
        self.table_schema: Schema = table_schema

    @staticmethod
    def write_field_mapping_local(
        mapping: FieldMapping.FieldMappingDict, filename: str
    ) -> bool:
        """Writes a field mapping as a .csv to a provided filename. """

        success = True
        # Always overwrite - assume that the mapping is supposed to the replace the current one
        with open(filename, mode="w+") as mapping_file:
            mapping_writer = csv.writer(mapping_file)
            mapping_writer.writerow(common.COLUMN_NAMES)
            for raw_text, (mapped_text, approved) in mapping.items():
                if mapped_text is None:
                    mapped_text = ""
                if approved is None:
                    approved = common.NOT_APPROVED
                success = success and mapping_writer.writerow(
                    [raw_text, mapped_text, approved]
                )
        return success

    @staticmethod
    def write_field_mappings_local(field_mappings: FieldMappings, config_location: str):
        """Writes all field mappings as a .csv to a provided directory."""
        dirname: str = os.path.dirname(
            config_location
        )  # TODO: are we sure we want dirname()?
        # Example: os.path.dirname(myparent/mydir) -> "myparent"
        if not os.path.exists(dirname):
            logging.error(
                "Invalid directory name provided: '%s' may not exist.", dirname
            )

        for field_name, field_mapping in field_mappings.items():
            field_mapping_dict = field_mapping.get_field_mapping_dict()
            if not field_mapping_dict:
                continue
            mapping_filename: str = common.get_field_mapping_filename(
                field_name, dirname
            )
            FieldMappingWriter.write_field_mapping_local(
                field_mapping_dict, mapping_filename
            )

    def _get_data_validation_requests(self, sheets):
        """
        Creates requests for setting data validations for the columns in
        field mapping sheets.

        Sets validations for:
        - The valid options for OUTPUT columns
        - The valid approval states for APPROVED columns
        """
        requests: List[Dict] = []

        for sheet in sheets:
            sheet_id = sheet["sheetId"]
            field_name = sheet["title"]
            options = get_enum_options(self.table_schema.get_field(field_name))

            # Add Output column validation
            requests.append(
                drive.get_data_validation_request(
                    sheet_id,
                    options,
                    common.OUTPUT_COLUMN_INDEX,
                    common.OUTPUT_COLUMN_INDEX,
                )
            )

            requests.append(
                drive.get_data_validation_request(
                    sheet_id,
                    common.VALID_APPROVED_VALUES,
                    common.APPROVED_COLUMN_INDEX,
                    common.APPROVED_COLUMN_INDEX,
                )
            )

        return requests

    def _get_auto_resize_requests(self, sheets):
        """
        Creates requests for auto resizing the columns in field mapping sheets.
        """
        requests: List[Dict] = []

        for sheet in sheets:
            sheet_id = sheet["sheetId"]
            requests.append(
                drive.get_auto_resize_request(
                    sheet_id, common.INPUT_COLUMN_INDEX, common.APPROVED_COLUMN_INDEX
                )
            )

        return requests

    def _get_batch_clear_mappings_body(self, field_mappings: FieldMappings):
        """
        Provides the body for batch clearing multiple ranges in a Google Sheet
        for the provided field mappings.
        """
        ranges: List[str] = [
            f"{field_name}!A:C" for field_name, _ in field_mappings.items()
        ]

        return {"ranges": ranges}

    def _get_batch_update_mappings_body(self, field_mappings: FieldMappings):
        """
        Provides the body for batch updating multiple ranges in a Google Sheet
        with provided field mappings.
        """
        data: List[Dict] = [
            {
                "range": f"{field_name}!A:C",
                "values": [common.COLUMN_NAMES]
                + field_mapping.get_field_mapping_df().values.tolist(),
            }
            for field_name, field_mapping in field_mappings.items()
        ]

        return {"valueInputOption": "RAW", "data": data}

    def write_field_mappings_drive(
        self, field_mappings: FieldMappings, account_info: Dict, spreadsheet_id: str
    ):
        """
        Writes all field mappings to a Google Sheet. Creates new sheets when
        required.

        Sets data validations on the columns and auto resizes column widths.
        """

        service: Resource = drive.get_google_sheets_service(account_info)

        # Get information on the existing sheets
        existing_sheets: List[SheetInfo] = drive.get_sheets_for_spreadsheet(
            service, spreadsheet_id
        )

        # Add sheets that don't exist yet
        existing_fields = drive.get_sheet_titles_from_sheets(existing_sheets)
        unwritten_fields: List[str] = [
            field for field in field_mappings.keys() if field not in existing_fields
        ]
        add_sheets_response = drive.add_sheets(
            service, unwritten_fields, spreadsheet_id
        )

        # Clear sheets
        batch_clear_mappings_body = self._get_batch_clear_mappings_body(field_mappings)
        drive.value_batch_clear(service, batch_clear_mappings_body, spreadsheet_id)

        # Update field mappings
        batch_update_mappings_body = self._get_batch_update_mappings_body(
            field_mappings
        )
        drive.value_batch_update(service, batch_update_mappings_body, spreadsheet_id)

        # Get information for sheets that were just added
        if add_sheets_response:
            new_sheets: List[SheetInfo] = [
                {
                    "title": reply["addSheet"]["properties"]["title"],
                    "sheetId": reply["addSheet"]["properties"]["sheetId"],
                }
                for reply in add_sheets_response["replies"]
            ]
        else:
            new_sheets: List[SheetInfo] = []

        all_valid_sheets = [
            sheet
            for sheet in existing_sheets + new_sheets
            if sheet["title"] in self.table_schema.field_names
        ]

        # Update field level validations (allowed enums and approved options)
        # and auto resize columns in spreadsheets
        data_validation_requests = self._get_data_validation_requests(all_valid_sheets)
        auto_resize_requests = self._get_auto_resize_requests(all_valid_sheets)

        requests = data_validation_requests + auto_resize_requests
        if requests:
            body = {"requests": requests}
            drive.batch_update(service, body, spreadsheet_id)


def airflow_write_field_mappings(
    credentials: Dict,
    spreadsheet_id: str,
    load_schema_xcom_args,
    resolved_field_mappings_xcom_args,
    ti,
    **kwargs,
) -> None:
    schema: Schema = ti.xcom_pull(**load_schema_xcom_args)
    resolved_field_mappings: FieldMappings = ti.xcom_pull(
        **resolved_field_mappings_xcom_args
    )

    if not resolved_field_mappings:
        return

    field_mapping_writer = FieldMappingWriter(schema)

    field_mapping_writer.write_field_mappings_drive(
        resolved_field_mappings, credentials, spreadsheet_id
    )
