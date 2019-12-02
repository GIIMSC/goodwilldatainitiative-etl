import pkg_resources
import unittest
from unittest.mock import patch
from tableschema import Schema
import pandas as pd

from etl.helpers.field_mapping.test_metadata import TEST_SCHEMA
from etl.helpers.field_mapping.loader import FieldMappingLoader


def load_sheets_as_dataframes_patch(
    service, spreadsheet_id, range, has_header_row=True
):
    return {
        "field1": pd.DataFrame(
            [["input1", "output2", "no"], ["input2", "output2", "no"]],
            columns=["Input", "Output", "Approved"],
        )
    }


def get_google_sheets_service_patch(account_info):
    return None


class FieldMappingLoaderTest(unittest.TestCase):
    def test_load_field_mappings_local_all_fields_mapped(self):
        field_mapping_loader = FieldMappingLoader(TEST_SCHEMA)

        config_dir = pkg_resources.resource_filename(
            "etl.helpers.test_files.configs1", "/"
        )
        actual_field_mappings = field_mapping_loader.load_field_mappings_local(
            config_dir
        )

        actual_field_mappings_dict = {
            field_name: field_mapping.get_field_mapping_dict()
            for field_name, field_mapping in actual_field_mappings.items()
        }

        expected_field_mappings = {
            "field1": {
                "thing": ("Hispanic/Latino ethnic origin", "Yes"),
                "option": ("Not Hispanic/Latino", "Yes"),
                "item": ("Not Hispanic/Latino", "No"),
            },
            "field2": {
                "thing": ("Blindness or Other Visual Impairment", "Yes"),
                "option": ("Deafness or Hard of Hearing", "No"),
                "item": ("Learning Disability other than Autism", "Yes"),
            },
            "field3": {"1-yes": ("yes", "Yes"), "2-no": ("no", "Yes")},
        }
        self.assertDictEqual(expected_field_mappings, actual_field_mappings_dict)

    def test_load_field_mappings_local_some_fields_not_mapped(self):
        field_mapping_loader = FieldMappingLoader(TEST_SCHEMA)

        config_dir = pkg_resources.resource_filename(
            "etl.helpers.test_files.configs2", "/"
        )
        actual_field_mappings = field_mapping_loader.load_field_mappings_local(
            config_dir
        )

        actual_field_mappings_dict = {
            field_name: field_mapping.get_field_mapping_dict()
            for field_name, field_mapping in actual_field_mappings.items()
        }

        expected_field_mappings = {
            "field1": {
                "thing": ("Hispanic/Latino ethnic origin", "Yes"),
                "option": ("Not Hispanic/Latino", "Yes"),
                "item": ("Not Hispanic/Latino", "No"),
            }
        }
        self.assertDictEqual(expected_field_mappings, actual_field_mappings_dict)

    @patch(
        "etl.helpers.drive.load_sheets_as_dataframes", load_sheets_as_dataframes_patch
    )
    @patch(
        "etl.helpers.drive.get_google_sheets_service", get_google_sheets_service_patch
    )
    def test_load_field_mappings_drive(self,):
        field_mapping_loader = FieldMappingLoader(TEST_SCHEMA)

        # Mocked out so we don't need real account info or sheet id
        loaded_field_mappings = field_mapping_loader.load_field_mappings_drive(
            None, None
        )

        expected_field_mapping_dict = {
            "input1": ("output1", "no"),
            "input2": ("output2", "no"),
        }

        self.assertTrue(
            expected_field_mapping_dict,
            loaded_field_mappings["field1"].get_field_mapping_dict(),
        )


if __name__ == "__main__":
    unittest.main()
