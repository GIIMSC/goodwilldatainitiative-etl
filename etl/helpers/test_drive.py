import unittest
import pandas as pd

from etl.helpers import drive

"""Unit tests for Google Drive helpers.

Run with `python -m etl.helpers.test_drive`.
"""


class DriveTest(unittest.TestCase):
    def test_get_data_validation_request(self):
        actual_request = drive.get_data_validation_request(
            sheet_id="1",
            values=["option1", "option2"],
            start_column_index=0,
            end_column_index=1,
        )

        expected_request = {
            "setDataValidation": {
                "range": {
                    "sheetId": "1",
                    "startRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": 2,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [
                            {"userEnteredValue": "option1"},
                            {"userEnteredValue": "option2"},
                        ],
                    },
                    "strict": True,
                    "showCustomUi": True,
                },
            }
        }

        self.assertEqual(expected_request, actual_request)

    def test_get_data_validation_request_no_values(self):
        actual_request = drive.get_data_validation_request(
            sheet_id="1", values=[], start_column_index=0, end_column_index=1
        )

        expected_request = {}

        self.assertEqual(expected_request, actual_request)

    def test_get_data_validation_request_no_start_column_index(self):
        actual_request = drive.get_data_validation_request(
            sheet_id="1",
            values=["option1", "option2"],
            start_column_index=None,
            end_column_index=1,
        )

        expected_request = {
            "setDataValidation": {
                "range": {"sheetId": "1", "startRowIndex": 1, "endColumnIndex": 2},
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [
                            {"userEnteredValue": "option1"},
                            {"userEnteredValue": "option2"},
                        ],
                    },
                    "strict": True,
                    "showCustomUi": True,
                },
            }
        }

        self.assertEqual(expected_request, actual_request)

    def test_get_data_validation_request_no_end_column_index(self):
        actual_request = drive.get_data_validation_request(
            sheet_id="1",
            values=["option1", "option2"],
            start_column_index=0,
            end_column_index=None,
        )

        expected_request = {
            "setDataValidation": {
                "range": {"sheetId": "1", "startRowIndex": 1, "startColumnIndex": 0},
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [
                            {"userEnteredValue": "option1"},
                            {"userEnteredValue": "option2"},
                        ],
                    },
                    "strict": True,
                    "showCustomUi": True,
                },
            }
        }

        self.assertEqual(expected_request, actual_request)

    def test_get_auto_resize_request(self):
        actual_request = drive.get_auto_resize_request(
            sheet_id="1", start_index=0, end_index=1
        )

        expected_request = {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": "1",
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": 2,
                }
            }
        }

        self.assertEqual(expected_request, actual_request)

    def test_get_auto_resize_request_no_start_index(self):
        actual_request = drive.get_auto_resize_request(
            sheet_id="1", start_index=None, end_index=1
        )

        expected_request = {
            "autoResizeDimensions": {
                "dimensions": {"sheetId": "1", "dimension": "COLUMNS", "endIndex": 2}
            }
        }

        self.assertEqual(expected_request, actual_request)

    def test_get_auto_resize_request_no_end_index(self):
        actual_request = drive.get_auto_resize_request(sheet_id="1", start_index=0)

        expected_request = {
            "autoResizeDimensions": {
                "dimensions": {"sheetId": "1", "dimension": "COLUMNS", "startIndex": 0}
            }
        }

        self.assertEqual(expected_request, actual_request)


if __name__ == "__main__":
    unittest.main()
