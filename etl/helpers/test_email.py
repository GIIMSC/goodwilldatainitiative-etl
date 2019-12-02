import unittest
import pandas as pd

from etl.helpers import common, data_processor, email
from etl.helpers.field_mapping.common import (
    FieldMapping,
    INPUT_COLUMN_NAME,
    OUTPUT_COLUMN_NAME,
    APPROVED_COLUMN_NAME,
)

"""Unit tests for email helpers.

Run with `python -m etl.helpers.test_email`.
"""


class EmailTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

    def test_format_validation_failures(self):
        failures = {
            common.EXPECT_VALUES_UNIQUE_KEY: {
                common.COLUMN_NAME_KEY: "Input",
                common.FAILED_VALUES_KEY: ["repeated_field", "other_field"],
            },
            common.EXPECT_VALUES_IN_SET_KEY: {
                common.COLUMN_NAME_KEY: "Output",
                common.FAILED_VALUES_KEY: ["invalid", "other_invalid"],
            },
            common.EXPECT_COLUMNS_IN_SET_KEY: {
                common.FAILED_VALUES_KEY: ["INVALID_COLUMN"]
            },
            common.EXPECT_COLUMNS_MATCH_KEY: {
                common.EXPECTED_ORDERED_LIST_KEY: ["Input", "Output", "Approved"],
                common.FAILED_VALUES_KEY: [None],
            },
        }

        self.assertEqual(
            '<ul><li>Some values for "Output" are invalid: invalid, other_invalid</li>'
            + '<li>No duplicates allowed for "Input". The following values had duplicates: repeated_field, other_field</li>'
            + "<li>Invalid column(s) in dataset: INVALID_COLUMN</li>"
            + "<li>Headers are incorrect. Header row (the first row in the sheet) should exactly match [Input, Output, Approved]</li></ul>",
            email.format_validation_failures(failures),
        )

    def test_format_unapproved_mappings(self):
        field1_mapping_df = pd.DataFrame(
            data={
                INPUT_COLUMN_NAME: ["input1", "input2"],
                OUTPUT_COLUMN_NAME: ["output1", "output2"],
                APPROVED_COLUMN_NAME: ["Yes", "Yes"],
            }
        )

        field2_mapping_df = pd.DataFrame(
            data={
                INPUT_COLUMN_NAME: ["in1", "in2"],
                OUTPUT_COLUMN_NAME: ["out1", "out2"],
                APPROVED_COLUMN_NAME: ["Yes", "No"],
            }
        )

        field_mappings = {
            "field1": FieldMapping.from_dataframe(field1_mapping_df),
            "field2": FieldMapping.from_dataframe(field2_mapping_df),
        }

        self.assertEqual(
            "<ul><li>field2</li><ul><li>'in2' <b>-></b> 'out2'</li></ul></ul>",
            email.format_unapproved_mappings(field_mappings),
        )

    def test_format_dropped_values(self):
        dropped_vals = [
            {
                data_processor.CASE_NUMBER_KEY: "case1",
                data_processor.FIELD_NAME_KEY: "TestField",
                data_processor.MILESTONE_FLAG_KEY: "Intake",
                data_processor.ORIGINAL_VALUE_KEY: "50",
                data_processor.INVALID_REASON_KEY: "50 is not in field mapping or valid value set",
            },
            {
                data_processor.CASE_NUMBER_KEY: "case2",
                data_processor.FIELD_NAME_KEY: "OtherTestField",
                data_processor.MILESTONE_FLAG_KEY: "Intake",
                data_processor.ORIGINAL_VALUE_KEY: "AAA",
                data_processor.INVALID_REASON_KEY: "AAA is not in field mapping or valid value set",
            },
        ]

        self.assertEqual(
            "<ul><li><i>(CaseNumber: 'case1', Milestone: 'Intake')</i> Invalid value for TestField: '50'"
            + "<ul><li>Reason: 50 is not in field mapping or valid value set</li></ul></li>"
            + "<li><i>(CaseNumber: 'case2', Milestone: 'Intake')</i> Invalid value for OtherTestField: 'AAA'"
            + "<ul><li>Reason: AAA is not in field mapping or valid value set</li></ul></li></ul>",
            email.format_dropped_vals(dropped_vals),
        )

    def test_format_dropped_rows(self):
        test_dataframe = pd.DataFrame(
            {
                "MilestoneFlag": ["Intake", None, "INVALID_FLAG"],
                "MemberOrganization": ["aaa-111", "aaa-111", "aaa-111"],
                "HourlyWage": ["$8.8", "$10.10", "$12.12"],
            }
        )

        dropped_rows = [
            {
                data_processor.ROW_KEY: test_dataframe.loc[0],
                data_processor.MISSING_FIELDS_KEY: ["CaseNumber"],
            },
            {
                data_processor.ROW_KEY: test_dataframe.loc[1],
                data_processor.MISSING_FIELDS_KEY: ["MilestoneFlag", "CaseNumber"],
            },
            {
                data_processor.ROW_KEY: test_dataframe.loc[2],
                data_processor.MISSING_FIELDS_KEY: ["MilestoneFlag", "CaseNumber"],
            },
        ]

        self.assertEqual(
            "<ul><li><i>(CaseNumber: 'None', MilestoneFlag: 'Intake')</i> Dropped because of CaseNumber (missing)</li>"
            + "<li><i>(CaseNumber: 'None', MilestoneFlag: 'None')</i> Dropped because of MilestoneFlag (missing), CaseNumber (missing)</li>"
            + "<li><i>(CaseNumber: 'None', MilestoneFlag: 'INVALID_FLAG')</i> Dropped because of MilestoneFlag (Invalid value: INVALID_FLAG), CaseNumber (missing)</li></ul>",
            email.format_dropped_rows(dropped_rows),
        )


if __name__ == "__main__":
    unittest.main()
