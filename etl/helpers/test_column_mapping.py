import logging
import unittest
import pandas as pd
from tableschema import Schema

from etl.helpers import common
from etl.helpers.column_mapping import ColumnMappingValidator

TEST_SCHEMA: Schema = Schema(
    {
        "fields": [
            {"name": "external_column_name1"},
            {"name": "external_column_name2"},
            {"name": "external_column_name3"},
        ]
    }
)

INTERNAL_COLUMN_NAME_COLUMN_NAME = "Internal Column Name"
GII_FIELD_NAME_COLUMN_NAME = "Mission Impact Field Name"


class ColumnMappingValidatorTest(unittest.TestCase):
    def test_column_mapping_has_correct_columns(self):
        column_mapping_validator = ColumnMappingValidator(TEST_SCHEMA)
        column_mapping_df = pd.DataFrame(
            data={
                INTERNAL_COLUMN_NAME_COLUMN_NAME: [
                    "internal_column_name1",
                    "internal_column_name2",
                ],
                GII_FIELD_NAME_COLUMN_NAME: [
                    "external_column_name1",
                    "external_column_name2",
                ],
            }
        )

        validation_failures = column_mapping_validator.validate(column_mapping_df)

        # No validation failures means that column mappings are valid.
        self.assertFalse(validation_failures)

    def test_column_mapping_is_missing_column(self):
        column_mapping_validator = ColumnMappingValidator(TEST_SCHEMA)
        column_mapping_df = pd.DataFrame(
            data={
                INTERNAL_COLUMN_NAME_COLUMN_NAME: [
                    "internal_column_name1",
                    "internal_column_name2",
                ]
            }
        )

        validation_failures = column_mapping_validator.validate(column_mapping_df)

        self.assertTrue(validation_failures)
        self.assertEqual(
            {
                "expect_table_columns_to_match_ordered_list": {
                    "expected_list": [
                        "Internal Column Name",
                        "Mission Impact Field Name",
                    ],
                    "failed_vals": [None],
                }
            },
            validation_failures,
        )

    def test_column_mapping_has_extra_column(self):
        column_mapping_validator = ColumnMappingValidator(TEST_SCHEMA)
        column_mapping_df = pd.DataFrame(
            data={
                INTERNAL_COLUMN_NAME_COLUMN_NAME: [
                    "internal_column_name1",
                    "internal_column_name2",
                ],
                GII_FIELD_NAME_COLUMN_NAME: [
                    "external_column_name1",
                    "external_column_name2",
                ],
                "random_column_name": [
                    "why is this here",
                    "what is this supposed to be",
                ],
            }
        )

        validation_failures = column_mapping_validator.validate(column_mapping_df)

        self.assertTrue(validation_failures)
        self.assertEqual(
            {
                "expect_table_columns_to_match_ordered_list": {
                    "expected_list": [
                        "Internal Column Name",
                        "Mission Impact Field Name",
                    ],
                    "failed_vals": ["random_column_name"],
                }
            },
            validation_failures,
        )

    def test_column_mapping_has_misnamed_column(self):
        column_mapping_validator = ColumnMappingValidator(TEST_SCHEMA)
        column_mapping_df = pd.DataFrame(
            data={
                "Internal Column Namewrong": [
                    "internal_column_name1",
                    "internal_column_name1",
                ],
                GII_FIELD_NAME_COLUMN_NAME: [
                    "external_column_name1",
                    "external_column_name2",
                ],
            }
        )

        validation_failures = column_mapping_validator.validate(column_mapping_df)

        self.assertTrue(validation_failures)
        self.assertEqual(
            {
                "expect_table_columns_to_match_ordered_list": {
                    "expected_list": [
                        "Internal Column Name",
                        "Mission Impact Field Name",
                    ],
                    "failed_vals": ["Internal Column Namewrong"],
                }
            },
            validation_failures,
        )

    def test_internal_column_names_are_only_mapped_once(self):
        column_mapping_validator = ColumnMappingValidator(TEST_SCHEMA)
        column_mapping_df = pd.DataFrame(
            data={
                INTERNAL_COLUMN_NAME_COLUMN_NAME: [
                    "internal_column_name1",
                    "internal_column_name2",
                ],
                GII_FIELD_NAME_COLUMN_NAME: [
                    "external_column_name1",
                    "external_column_name2",
                ],
            }
        )

        validation_failures = column_mapping_validator.validate(column_mapping_df)

        self.assertFalse(validation_failures)

    def test_internal_column_names_are_mapped_multiple_times(self):
        column_mapping_validator = ColumnMappingValidator(TEST_SCHEMA)
        column_mapping_df = pd.DataFrame(
            data={
                INTERNAL_COLUMN_NAME_COLUMN_NAME: [
                    "internal_column_name1",
                    "internal_column_name1",
                ],
                GII_FIELD_NAME_COLUMN_NAME: [
                    "external_column_name1",
                    "external_column_name2",
                ],
            }
        )

        validation_failures = column_mapping_validator.validate(column_mapping_df)

        self.assertTrue(validation_failures)
        self.assertEqual(
            {
                "expect_column_values_to_be_unique": {
                    "column_name": "Internal Column Name",
                    "failed_vals": ["internal_column_name1"],
                }
            },
            validation_failures,
        )

    def test_external_column_names_are_valid(self):
        column_mapping_validator = ColumnMappingValidator(TEST_SCHEMA)
        column_mapping_df = pd.DataFrame(
            data={
                INTERNAL_COLUMN_NAME_COLUMN_NAME: [
                    "internal_column_name1",
                    "internal_column_name2",
                ],
                GII_FIELD_NAME_COLUMN_NAME: [
                    "external_column_name1",
                    "external_column_name2",
                ],
            }
        )

        validation_failures = column_mapping_validator.validate(column_mapping_df)

        self.assertFalse(validation_failures)

    def test_external_column_names_are_invalid(self):
        column_mapping_validator = ColumnMappingValidator(TEST_SCHEMA)
        column_mapping_df = pd.DataFrame(
            data={
                INTERNAL_COLUMN_NAME_COLUMN_NAME: [
                    "internal_column_name1",
                    "internal_column_name2",
                ],
                GII_FIELD_NAME_COLUMN_NAME: [
                    "external_column_name1",
                    "external_column_name_not_real",
                ],
            }
        )

        validation_failures = column_mapping_validator.validate(column_mapping_df)

        self.assertTrue(validation_failures)
        self.assertEqual(
            {
                "expect_column_values_to_be_in_set": {
                    "column_name": "Mission Impact Field Name",
                    "failed_vals": ["external_column_name_not_real"],
                }
            },
            validation_failures,
        )


if __name__ == "__main__":
    unittest.main()
