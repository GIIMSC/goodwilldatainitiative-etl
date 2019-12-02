import unittest
import pandas as pd

from etl.helpers.common import (
    EXPECT_VALUES_IN_SET_KEY,
    EXPECT_VALUES_UNIQUE_KEY,
    EXPECT_COLUMNS_MATCH_KEY,
    COLUMN_NAME_KEY,
    FAILED_VALUES_KEY,
    EXPECTED_ORDERED_LIST_KEY,
)
from etl.helpers import table_schema
from etl.helpers.field_mapping import common
from etl.helpers.field_mapping.common import FieldMapping
from etl.helpers.field_mapping.validator import (
    FieldMappingValidator,
    FieldMappingApprovalValidator,
)

"""Unit tests for FieldMappingValidator and FieldMappingApprovalValidator.

Run with `python -m etl.helpers.field_mapping.test_validator`.
"""


class FieldMappingValidatorTest(unittest.TestCase):
    def test_validate_valid(self):

        schema = table_schema.get_schema("etl/schemas/mission_impact_table_schema.json")
        field_mapping_validator = FieldMappingValidator(schema)
        categories_mapping = pd.DataFrame(
            data={
                common.INPUT_COLUMN_NAME: ["dislocated", "lack literacy"],
                common.OUTPUT_COLUMN_NAME: ["Dislocated Worker", "Lack of Literacy"],
                common.APPROVED_COLUMN_NAME: ["Yes", "Yes"],
            }
        )

        savings_mapping = pd.DataFrame(
            data={
                common.INPUT_COLUMN_NAME: ["yes, has savings", "no savings"],
                common.OUTPUT_COLUMN_NAME: ["yes", "no"],
                common.APPROVED_COLUMN_NAME: ["Yes", "Yes"],
            }
        )

        milestone_mapping = pd.DataFrame(
            data={
                common.INPUT_COLUMN_NAME: ["intake form", "90 Days"],
                common.OUTPUT_COLUMN_NAME: ["Intake", "NinetyDays"],
                common.APPROVED_COLUMN_NAME: ["Yes", "Yes"],
            }
        )

        validation_failures = field_mapping_validator.validate_multiple(
            {
                "CategoriesIdentifyWith": FieldMapping.from_dataframe(
                    categories_mapping
                ),
                "HasSavings": FieldMapping.from_dataframe(savings_mapping),
                "MilestoneFlag": FieldMapping.from_dataframe(milestone_mapping),
            }
        )

        self.assertFalse(validation_failures)

    def test_validate_invalid(self):

        schema = table_schema.get_schema("etl/schemas/mission_impact_table_schema.json")
        field_mapping_validator = FieldMappingValidator(schema)
        categories_mapping = pd.DataFrame(
            data={
                common.INPUT_COLUMN_NAME: ["dislocated", "dislocated"],
                common.OUTPUT_COLUMN_NAME: ["Dislocated Worker", "Lack of Literacy"],
                common.APPROVED_COLUMN_NAME: ["Yes", "Yes"],
            }
        )

        savings_mapping = pd.DataFrame(
            data={
                common.INPUT_COLUMN_NAME: ["yes, has savings", "no savings"],
                common.OUTPUT_COLUMN_NAME: ["invalid", "no"],
                common.APPROVED_COLUMN_NAME: ["Yes", "Yes"],
            }
        )

        milestone_mapping = pd.DataFrame(
            data={
                common.INPUT_COLUMN_NAME: ["intake form", "90 Days"],
                common.OUTPUT_COLUMN_NAME: ["Intake", "NinetyDays"],
            }
        )

        validation_failures = field_mapping_validator.validate_multiple(
            {
                "CategoriesIdentifyWith": FieldMapping.from_dataframe(
                    categories_mapping
                ),
                "HasSavings": FieldMapping.from_dataframe(savings_mapping),
                "MilestoneFlag": FieldMapping.from_dataframe(milestone_mapping),
            }
        )

        self.assertTrue(validation_failures)

        self.assertEquals(
            {
                "CategoriesIdentifyWith": {
                    EXPECT_VALUES_UNIQUE_KEY: {
                        FAILED_VALUES_KEY: ["dislocated"],
                        COLUMN_NAME_KEY: common.INPUT_COLUMN_NAME,
                    }
                },
                "HasSavings": {
                    EXPECT_VALUES_IN_SET_KEY: {
                        FAILED_VALUES_KEY: ["invalid"],
                        COLUMN_NAME_KEY: common.OUTPUT_COLUMN_NAME,
                    }
                },
                "MilestoneFlag": {
                    EXPECT_COLUMNS_MATCH_KEY: {
                        FAILED_VALUES_KEY: [None],
                        EXPECTED_ORDERED_LIST_KEY: [
                            common.INPUT_COLUMN_NAME,
                            common.OUTPUT_COLUMN_NAME,
                            common.APPROVED_COLUMN_NAME,
                        ],
                    }
                },
            },
            validation_failures,
        )


class FieldMappingApprovalValidatorTest(unittest.TestCase):
    def test_validate_all_approved_should_pass(self):
        field_mapping_approval_validator = FieldMappingApprovalValidator()
        field_mapping_df = pd.DataFrame(
            data={
                common.INPUT_COLUMN_NAME: ["input1", "input2"],
                common.OUTPUT_COLUMN_NAME: ["output1", "output2"],
                common.APPROVED_COLUMN_NAME: ["Yes", "Yes"],
            }
        )

        validation_failures = field_mapping_approval_validator.validate_multiple(
            {"test_field": FieldMapping.from_dataframe(field_mapping_df)}
        )

        self.assertFalse(validation_failures)

    def test_validate_at_least_one_not_approved_should_fail(self):
        field_mapping_approval_validator = FieldMappingApprovalValidator()
        field_mapping_df = pd.DataFrame(
            data={
                common.INPUT_COLUMN_NAME: ["input1", "input2"],
                common.OUTPUT_COLUMN_NAME: ["output1", "output2"],
                common.APPROVED_COLUMN_NAME: ["Yes", "No"],
            }
        )

        validation_failures = field_mapping_approval_validator.validate_multiple(
            {"test_field": FieldMapping.from_dataframe(field_mapping_df)}
        )

        self.assertTrue(validation_failures)
        self.assertEquals(
            {
                "test_field": {
                    "expect_column_values_to_be_in_set": {
                        "column_name": "Approved",
                        "failed_vals": ["No"],
                    }
                }
            },
            validation_failures,
        )


if __name__ == "__main__":
    unittest.main()
