import os
import shutil
import unittest
import pandas as pd
import tempfile
from unittest.mock import patch

from etl.helpers.field_mapping import common
from etl.helpers.field_mapping.common import FieldMapping
from etl.helpers.field_mapping.writer import FieldMappingWriter
from etl.helpers.field_mapping.test_metadata import TEST_SCHEMA


class FieldMappingWriterTest(unittest.TestCase):
    def setUp(self):
        self.dirname = (
            tempfile.mkdtemp() + "/"
        )  # Add slash for write_mappings() behavior

    def tearDown(self):
        shutil.rmtree(self.dirname)

    def test_write_field_mappings_local(self):
        field_mappings = {
            "field1": FieldMapping.from_dict(
                {
                    "thing": ("Hispanic/Latino ethnic origin", "No"),
                    "option": ("Not Hispanic/Latino", "Yes"),
                    "item": ("Not Hispanic/Latino", "No"),
                }
            ),
            "field2": FieldMapping.from_dict(
                {
                    "thing": ("Blindness or Other Visual Impairment", "No"),
                    "option": ("Deafness or Hard of Hearing", None),
                    "item": ("Learning Disability other than Autism", "No"),
                }
            ),
            "field3": FieldMapping.from_dict(
                {"1-yes": ("yes", "No"), "2-no": ("no", "No")}
            ),
        }

        FieldMappingWriter.write_field_mappings_local(field_mappings, self.dirname)

        actual_field1_dataframe = pd.read_csv(
            os.path.join(self.dirname, "field1.csv"), header=None
        )
        actual_field2_dataframe = pd.read_csv(
            os.path.join(self.dirname, "field2.csv"), header=None
        )
        actual_field3_dataframe = pd.read_csv(
            os.path.join(self.dirname, "field3.csv"), header=None
        )

        expected_field1_dataframe = pd.DataFrame(
            data=[
                common.COLUMN_NAMES,
                ["thing", "Hispanic/Latino ethnic origin", "No"],
                ["option", "Not Hispanic/Latino", "Yes"],
                ["item", "Not Hispanic/Latino", "No"],
            ]
        )
        expected_field2_dataframe = pd.DataFrame(
            data=[
                common.COLUMN_NAMES,
                ["thing", "Blindness or Other Visual Impairment", "No"],
                ["option", "Deafness or Hard of Hearing", "No"],
                ["item", "Learning Disability other than Autism", "No"],
            ]
        )
        expected_field3_dataframe = pd.DataFrame(
            data=[common.COLUMN_NAMES, ["1-yes", "yes", "No"], ["2-no", "no", "No"]]
        )

        pd.testing.assert_frame_equal(
            expected_field1_dataframe, actual_field1_dataframe
        )
        pd.testing.assert_frame_equal(
            expected_field2_dataframe, actual_field2_dataframe
        )
        pd.testing.assert_frame_equal(
            expected_field3_dataframe, actual_field3_dataframe
        )

    @patch("etl.helpers.drive.batch_update")
    @patch("etl.helpers.drive.value_batch_update")
    @patch("etl.helpers.drive.value_batch_clear")
    @patch("etl.helpers.drive.add_sheets")
    @patch("etl.helpers.drive.get_sheets_for_spreadsheet")
    @patch("etl.helpers.drive.get_google_sheets_service")
    def test_write_field_mappings_drive(
        self,
        _,
        get_sheets_for_spreadsheet_patch,
        add_sheets_patch,
        value_batch_clear_patch,
        value_batch_update_patch,
        batch_update_patch,
    ):
        get_sheets_for_spreadsheet_patch.return_value = [
            {"title": "field1", "sheetId": "id1"}
        ]

        add_sheets_patch.return_value = {
            "replies": [
                {"addSheet": {"properties": {"title": "field2", "sheetId": "id2"}}}
            ]
        }
        field_mapping_writer = FieldMappingWriter(TEST_SCHEMA)

        field_mappings = {
            "field1": FieldMapping.from_dict({"input1": ("output1", "no")}),
            "field2": FieldMapping.from_dict({"input1": ("output1", "no")}),
        }
        field_mapping_writer.write_field_mappings_drive(field_mappings, None, None)

        # Check that add sheets call was sent
        add_sheets_patch.assert_called_with(
            unittest.mock.ANY, ["field2"], unittest.mock.ANY
        )

        # Check that batch clear was sent
        value_batch_clear_patch.assert_called_with(
            unittest.mock.ANY,
            {"ranges": ["field1!A:C", "field2!A:C"]},
            unittest.mock.ANY,
        )

        # Check that the batch update was sent
        value_batch_update_patch.assert_called_with(
            unittest.mock.ANY,
            {
                "valueInputOption": "RAW",
                "data": [
                    {
                        "range": "field1!A:C",
                        "values": [
                            ["Input", "Output", "Approved"],
                            ["input1", "output1", "no"],
                        ],
                    },
                    {
                        "range": "field2!A:C",
                        "values": [
                            ["Input", "Output", "Approved"],
                            ["input1", "output1", "no"],
                        ],
                    },
                ],
            },
            unittest.mock.ANY,
        )

        # Check that the validation and auto resize requests were sent
        valid_sheets = [
            {"title": "field1", "sheetId": "id1"},
            {"title": "field2", "sheetId": "id2"},
        ]
        batch_update_patch.assert_called_with(
            unittest.mock.ANY,
            {
                "requests": [
                    {
                        "setDataValidation": {
                            "range": {
                                "sheetId": "id1",
                                "startRowIndex": 1,
                                "startColumnIndex": 1,
                                "endColumnIndex": 2,
                            },
                            "rule": {
                                "condition": {
                                    "type": "ONE_OF_LIST",
                                    "values": [
                                        {
                                            "userEnteredValue": "Hispanic/Latino ethnic origin"
                                        },
                                        {"userEnteredValue": "Not Hispanic/Latino"},
                                    ],
                                },
                                "strict": True,
                                "showCustomUi": True,
                            },
                        }
                    },
                    {
                        "setDataValidation": {
                            "range": {
                                "sheetId": "id1",
                                "startRowIndex": 1,
                                "startColumnIndex": 2,
                                "endColumnIndex": 3,
                            },
                            "rule": {
                                "condition": {
                                    "type": "ONE_OF_LIST",
                                    "values": [
                                        {"userEnteredValue": "Yes"},
                                        {"userEnteredValue": "No"},
                                    ],
                                },
                                "strict": True,
                                "showCustomUi": True,
                            },
                        }
                    },
                    {
                        "setDataValidation": {
                            "range": {
                                "sheetId": "id2",
                                "startRowIndex": 1,
                                "startColumnIndex": 1,
                                "endColumnIndex": 2,
                            },
                            "rule": {
                                "condition": {
                                    "type": "ONE_OF_LIST",
                                    "values": [
                                        {
                                            "userEnteredValue": "Blindness or Other Visual Impairment"
                                        },
                                        {
                                            "userEnteredValue": "Deafness or Hard of Hearing"
                                        },
                                        {
                                            "userEnteredValue": "Other Physical Disability"
                                        },
                                        {"userEnteredValue": "Neurological Disability"},
                                        {
                                            "userEnteredValue": "Learning Disability other than Autism"
                                        },
                                        {
                                            "userEnteredValue": "Developmental Disability other than Autism"
                                        },
                                        {"userEnteredValue": "Autism"},
                                        {"userEnteredValue": "Psychiatric Disability"},
                                        {"userEnteredValue": "Emotional Disability"},
                                        {
                                            "userEnteredValue": "Other disabling condition"
                                        },
                                    ],
                                },
                                "strict": True,
                                "showCustomUi": True,
                            },
                        }
                    },
                    {
                        "setDataValidation": {
                            "range": {
                                "sheetId": "id2",
                                "startRowIndex": 1,
                                "startColumnIndex": 2,
                                "endColumnIndex": 3,
                            },
                            "rule": {
                                "condition": {
                                    "type": "ONE_OF_LIST",
                                    "values": [
                                        {"userEnteredValue": "Yes"},
                                        {"userEnteredValue": "No"},
                                    ],
                                },
                                "strict": True,
                                "showCustomUi": True,
                            },
                        }
                    },
                    {
                        "autoResizeDimensions": {
                            "dimensions": {
                                "sheetId": "id1",
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex": 3,
                            }
                        }
                    },
                    {
                        "autoResizeDimensions": {
                            "dimensions": {
                                "sheetId": "id2",
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex": 3,
                            }
                        }
                    },
                ]
            },
            unittest.mock.ANY,
        )


if __name__ == "__main__":
    unittest.main()
