"""Tests for Pipeline.

Run from root of repo with `python -m etl.pipeline.test_simple_pipeline.py`.
"""
import os
import pkg_resources
import shutil
import unittest
import pandas as pd
from typing import Dict

from etl.pipeline import simple_pipeline

MEMBER_ID = "member_id"
MULTIPLE_VAL_DELIMITER = ";"

TEMP_MAPPING_DIR = "temp_mappings/"
TEST_DIR = pkg_resources.resource_filename("testfiles", "")
SCHEMA_DIR = pkg_resources.resource_filename("etl.schemas", "")

MI_TEST_DIR = os.path.join(TEST_DIR, "mission_impact/")
MI_SCHEMA = os.path.join(SCHEMA_DIR, "mission_impact_table_schema.json")
MI_DATAFILE = os.path.join(MI_TEST_DIR, "fake_data.csv")
MI_COL_MAPPINGS = os.path.join(MI_TEST_DIR, "fake_column_mapping.csv")
MI_MAPPINGS_INPUT_DIR = os.path.join(MI_TEST_DIR, "initial_mappings/")


class TestPipeline(unittest.TestCase):
    def setUp(self):
        os.mkdir(TEMP_MAPPING_DIR)

    def tearDown(self):
        shutil.rmtree(TEMP_MAPPING_DIR)

    def test_missionImpact(self):
        """
        This test runs 'fake_data.csv' through the simple_pipeline.
        'fake_data.csv' (and the corresponding column and field mappings) should not raise any errors, and so, the
        assertions check for expected values in 'email_metadata' and 'dataset'.
        """
        pipeline_return_vals = simple_pipeline.from_local(
            member_id=MEMBER_ID,
            row_format=True,
            schema_filename=MI_SCHEMA,
            multiple_val_delimiter=MULTIPLE_VAL_DELIMITER,
            column_mapping_filename=MI_COL_MAPPINGS,
            field_mappings_filename=MI_MAPPINGS_INPUT_DIR,
            extracted_data_filenames=[MI_DATAFILE],
        )

        resolved_field_mappings = pipeline_return_vals["field_mappings"]
        self.assertIsInstance(resolved_field_mappings, Dict)

        # fake_data.csv contains six rows (with two duplicates), and so, we expect only four rows in 'num_rows_to_upload'.
        assert pipeline_return_vals["email_metadata"]["num_rows_to_upload"] == 4
        assert pipeline_return_vals["email_metadata"]["dropped_rows"] == []
        assert pipeline_return_vals["email_metadata"]["dropped_values"] == []

        expected_case_nums = pd.Series(
            ["CASEID-000001", "CASEID-000002", "CASEID-000003", "CASEID-000004"]
        )
        case_nums = pipeline_return_vals["dataset"]["CaseNumber"]
        pd.testing.assert_series_equal(
            case_nums, expected_case_nums, check_index_type=False, check_names=False
        )


if __name__ == "__main__":
    unittest.main()
