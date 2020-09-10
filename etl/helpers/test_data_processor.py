import datetime
from decimal import Decimal
import unittest
import pandas as pd

from etl.helpers.field_mapping.common import FieldMapping
from etl.helpers import data_processor, table_schema

"""Unit tests for CellProcessor.

Run with `python -m etl.helpers.test_data_processor`.
"""

FAKE_DATA = pd.DataFrame(
    {
        "MilestoneFlag": ["Intake", "TwoYears", "Exit", "Intake", "Midpoint"],
        "CaseNumber": ["CN-1", "CN-2", "CN-3", "CN-4", "CN-5"],
        "MemberOrganization": ["aaa-111", "aaa-111", "aaa-111", "aaa-111", "aaa-111"],
        "HourlyWage": ["8.8", "12.12", "15.00", "", None],
        "HouseholdIncome": ["20,000.00", "30,000", "15000.88", "", None],
        "StackableCredentials": ["1", "6.0", "2", "", None],
        "SOC": ["15-2030  ", "15-2030.04", "15-2030", "", None],
        "State": ["NC", "wa", "Kentucky", "  ", None],
        "DateOfBirth": ["1-5-1982", "1/5/1956", "1-10-1986", "", None],
        "CategoriesIdentifyWith": [["5", "3"], [1], [""], [5, 3], None],
        "ConvictedInLastYear": ["yes", "no", "true", "", None],
        "SelfEfficacyScore1": ["5", 3, "Agree", "", None],
    }
)

EXPECTED_OUTPUT = pd.DataFrame(
    {
        "MilestoneFlag": ["Intake", "TwoYears", "Exit", "Intake", "Midpoint"],
        "CaseNumber": ["CN-1", "CN-2", "CN-3", "CN-4", "CN-5"],
        "MemberOrganization": ["aaa-111", "aaa-111", "aaa-111", "aaa-111", "aaa-111"],
        "HourlyWage": [Decimal("8.8"), Decimal("12.12"), Decimal("15.00"), None, None],
        "HouseholdIncome": [
            Decimal(20000.00),
            Decimal(30000),
            Decimal(15000.88).quantize(Decimal(".01")),
            None,
            None,
        ],
        "StackableCredentials": [1, 6, 2, None, None],
        "SOC": ["15-2030", "15-2030", "15-2030", None, None],
        "State": ["NC", "WA", "KY", None, None],
        "DateOfBirth": [
            datetime.date(1982, 1, 5),
            datetime.date(1956, 1, 5),
            datetime.date(1986, 1, 10),
            None,
            None,
        ],
        "CategoriesIdentifyWith": [[5, 3], [1], None, [5, 3], None],
        "ConvictedInLastYear": [True, False, True, None, None],
        "SelfEfficacyScore1": [5, 3, 2, None, None],
    },
    dtype=object,
)

FAKE_FIELD_MAPPINGS = {
    # MilestoneFlag is a non-numeric enum.
    "MilestoneFlag": FieldMapping.from_dict(
        {
            "almost_Intake": ("Intake", "Yes"),
            "two years": ("TwoYears", "Yes"),
            "not_a_match": ("", "Yes"),
        }
    ),
    # CategoriesIdentifyWith is a numeric enum that allows multiple values.
    "CategoriesIdentifyWith": FieldMapping.from_dict(
        {
            "Substance abuse": ("History of Substance Abuse", "Yes"),
            "English as a Second Language": (
                "Non-English Speaker/English as a Second Language",
                "Yes",
            ),
            "LGBT": ("LGBTQ", "Yes"),
            "not_a_match": (None, "Yes"),
        }
    ),
}


def _create_value_identifer(value, field_name, case_number, milestone_flag, reasons):
    return {
        data_processor.CASE_NUMBER_KEY: case_number,
        data_processor.FIELD_NAME_KEY: field_name,
        data_processor.MILESTONE_FLAG_KEY: milestone_flag,
        data_processor.ORIGINAL_VALUE_KEY: value,
        data_processor.INVALID_REASON_KEY: reasons,
    }


def _create_row_identifer(row, invalid_fields):
    return {
        data_processor.ROW_KEY: row,
        data_processor.MISSING_FIELDS_KEY: invalid_fields,
    }


class TestDataProcessor(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        # TODO: Use pkg_resources instead of relative paths.
        self.schema = table_schema.get_schema(
            "etl/schemas/mission_impact_table_schema.json"
        )
        self.processor = data_processor.DataProcessor(FAKE_FIELD_MAPPINGS, self.schema)
        self.input_df = FAKE_DATA.copy()
        self.expected_df = EXPECTED_OUTPUT.copy()

    def test_handleValidData(self):
        output_df, invalid_values, dropped_rows = self.processor.process(FAKE_DATA)

        # Check that no values are invalid.
        self.assertFalse(invalid_values)
        self.assertFalse(dropped_rows)
        pd.util.testing.assert_frame_equal(EXPECTED_OUTPUT, output_df)

    def test_handleInvalid_soc(self):
        self.input_df["SOC"] = [4, "15-2030.04", "1511-2030.00", "", None]
        output_df, invalid_values, dropped_rows = self.processor.process(self.input_df)
        self.assertEqual(
            [
                _create_value_identifer(
                    4, "SOC", "CN-1", "Intake", "4 is not a string"
                ),
                _create_value_identifer(
                    "1511-2030.00",
                    "SOC",
                    "CN-3",
                    "Exit",
                    "SOC should be in the format ##-####",
                ),
            ],
            invalid_values,
        )
        self.assertFalse(dropped_rows)

        self.expected_df["SOC"] = pd.Series(
            [None, "15-2030", None, None, None], dtype=object
        )
        pd.util.testing.assert_frame_equal(self.expected_df, output_df)

    def test_handleInvalid_state(self):
        self.input_df["State"] = ["NC", "RANDOM STATE", 7, "", None]
        output_df, invalid_values, dropped_rows = self.processor.process(self.input_df)
        self.assertEqual(
            [
                _create_value_identifer(
                    "RANDOM STATE",
                    "State",
                    "CN-2",
                    "TwoYears",
                    "RANDOM STATE is not a valid state",
                ),
                _create_value_identifer(
                    7, "State", "CN-3", "Exit", "7 is not a valid state"
                ),
            ],
            invalid_values,
        )
        self.assertFalse(dropped_rows)

        self.expected_df["State"] = pd.Series(
            ["NC", None, None, None, None], dtype=object
        )
        pd.util.testing.assert_frame_equal(self.expected_df, output_df)

    def test_handleInvalid_intEnum(self):
        self.input_df["SelfEfficacyScore1"] = [1, 10, "wrong", 0, None]
        output_df, invalid_values, dropped_rows = self.processor.process(self.input_df)
        self.assertEqual(
            [
                _create_value_identifer(
                    "wrong",
                    "SelfEfficacyScore1",
                    "CN-3",
                    "Exit",
                    "wrong is not in field mapping or valid value set",
                ),
                _create_value_identifer(
                    10,
                    "SelfEfficacyScore1",
                    "CN-2",
                    "TwoYears",
                    "SelfEfficacyScore1 must be within the range [1, 5]",
                ),
                _create_value_identifer(
                    0,
                    "SelfEfficacyScore1",
                    "CN-4",
                    "Intake",
                    "SelfEfficacyScore1 must be within the range [1, 5]",
                ),
            ],
            invalid_values,
        )
        self.assertFalse(dropped_rows)

        self.expected_df["SelfEfficacyScore1"] = pd.Series(
            [1, None, None, None, None], dtype=object
        )
        pd.util.testing.assert_frame_equal(self.expected_df, output_df)

    def test_handleInvalid_boolean(self):
        self.input_df["ConvictedInLastYear"] = ["aa", "t", 1, 7, None]
        output_df, invalid_values, dropped_rows = self.processor.process(self.input_df)
        self.assertEqual(
            [
                _create_value_identifer(
                    "aa",
                    "ConvictedInLastYear",
                    "CN-1",
                    "Intake",
                    "aa is not in field mapping or valid value set (['yes', 'Y', 'true', 'T', '1', 'no', 'N', 'false', 'F', '2'])",
                ),
                _create_value_identifer(
                    7,
                    "ConvictedInLastYear",
                    "CN-4",
                    "Intake",
                    "7 is not in field mapping or valid value set (['yes', 'Y', 'true', 'T', '1', 'no', 'N', 'false', 'F', '2'])",
                ),
            ],
            invalid_values,
        )
        self.assertFalse(dropped_rows)

        self.expected_df["ConvictedInLastYear"] = pd.Series(
            [None, True, True, None, None], dtype=object
        )
        pd.util.testing.assert_frame_equal(self.expected_df, output_df)

    def test_handleInvalid_date(self):
        self.input_df["DateOfBirth"] = [
            "invalid_date",
            "1/5/1956",
            "SOME DAY",
            "",
            None,
        ]
        self.input_df["DateOfBirth"] = [
            "invalid_date",
            "1/5/1956",
            "SOME DAY",
            "",
            None,
        ]
        output_df, invalid_values, dropped_rows = self.processor.process(self.input_df)
        self.assertEqual(
            [
                _create_value_identifer(
                    "invalid_date",
                    "DateOfBirth",
                    "CN-1",
                    "Intake",
                    "invalid_date is not a valid date",
                ),
                _create_value_identifer(
                    "SOME DAY",
                    "DateOfBirth",
                    "CN-3",
                    "Exit",
                    "SOME DAY is not a valid date",
                ),
            ],
            invalid_values,
        )
        self.assertFalse(dropped_rows)

        self.expected_df["DateOfBirth"] = pd.Series(
            [None, datetime.date(1956, 1, 5), None, None, None], dtype=object
        )
        pd.util.testing.assert_frame_equal(self.expected_df, output_df)

    def test_handleInvalid_invalidMultipleValue(self):
        self.input_df["CategoriesIdentifyWith"] = [["5", "g"], "1", [""], [5, 3], None]
        output_df, invalid_values, dropped_rows = self.processor.process(self.input_df)
        self.assertEqual(
            [
                _create_value_identifer(
                    ["5", "g"],
                    "CategoriesIdentifyWith",
                    "CN-1",
                    "Intake",
                    "g is not in field mapping or valid value set",
                ),
                _create_value_identifer(
                    "1", "CategoriesIdentifyWith", "CN-2", "TwoYears", "1 is not a list"
                ),
            ],
            invalid_values,
        )
        self.assertFalse(dropped_rows)

        self.expected_df["CategoriesIdentifyWith"] = [None, None, None, [5, 3], None]
        pd.util.testing.assert_frame_equal(self.expected_df, output_df)

    def test_handleDroppedRowsAndValues(self):
        self.input_df["CaseNumber"] = ["CN-1", "CN-2", "", "CN-4", "CN-5"]
        self.input_df["MilestoneFlag"] = [
            None,
            "TwoYears",
            "Exit",
            "Intake",
            "Midpoint",
        ]
        self.input_df["MilestoneFlag"] = [
            None,
            "TwoYears",
            "Exit",
            "Intake",
            "Midpoint",
        ]
        self.input_df["StackableCredentials"] = ["4", "-1", "40", "3", "74"]
        output_df, invalid_values, dropped_rows = self.processor.process(self.input_df)

        # Does not include invalid values for rows that are dropped.
        self.assertEqual(
            [
                _create_value_identifer(
                    "-1",
                    "StackableCredentials",
                    "CN-2",
                    "TwoYears",
                    "StackableCredentials must be within the range [0, 9]",
                ),
                _create_value_identifer(
                    "74",
                    "StackableCredentials",
                    "CN-5",
                    "Midpoint",
                    "StackableCredentials must be within the range [0, 9]",
                ),
            ],
            invalid_values,
        )

        self.assertTrue(
            dropped_rows[0][data_processor.ROW_KEY].equals(self.input_df.loc[0])
        )
        self.assertTrue(
            dropped_rows[1][data_processor.ROW_KEY].equals(self.input_df.loc[2])
        )
        self.assertEqual(
            dropped_rows[0][data_processor.MISSING_FIELDS_KEY], ["MilestoneFlag"]
        )
        self.assertEqual(
            dropped_rows[1][data_processor.MISSING_FIELDS_KEY], ["CaseNumber"]
        )

        self.expected_df = self.expected_df.drop([0, 2]).reset_index(drop=True)
        self.expected_df["StackableCredentials"] = pd.Series(
            [None, 3, None], dtype=object
        )
        pd.util.testing.assert_frame_equal(self.expected_df, output_df)

    def test_handleRequired_emptyRequiredFields(self):
        self.input_df["CaseNumber"] = ["CN-1", "CN-2", "CN-3", "", "CN-5"]
        self.input_df["MilestoneFlag"] = ["Intake", "TwoYears", "Exit", None, None]
        output_df, invalid_values, dropped_rows = self.processor.process(self.input_df)

        self.expected_df.drop([3, 4], inplace=True)

        self.assertFalse(invalid_values)

        # Check dropped rows.
        self.assertTrue(
            dropped_rows[0][data_processor.ROW_KEY].equals(self.input_df.loc[3])
        )
        self.assertTrue(
            dropped_rows[1][data_processor.ROW_KEY].equals(self.input_df.loc[4])
        )
        self.assertEqual(
            dropped_rows[0][data_processor.MISSING_FIELDS_KEY],
            ["MilestoneFlag", "CaseNumber"],
        )
        self.assertEqual(
            dropped_rows[1][data_processor.MISSING_FIELDS_KEY], ["MilestoneFlag"]
        )

        pd.util.testing.assert_frame_equal(self.expected_df, output_df)

    def test_handleRequired_unmappabbleMilestoneFlag(self):
        self.input_df["MilestoneFlag"] = ["placeholder_string", "", 3, "3.0", "333"]
        output_df, invalid_values, dropped_rows = self.processor.process(self.input_df)
        self.assertEqual(
            [
                _create_value_identifer(
                    "placeholder_string",
                    "MilestoneFlag",
                    "CN-1",
                    "placeholder_string",
                    "placeholder_string is not in field mapping or valid value set",
                ),
                _create_value_identifer(
                    3,
                    "MilestoneFlag",
                    "CN-3",
                    3,
                    "3 is not in field mapping or valid value set",
                ),
                _create_value_identifer(
                    "3.0",
                    "MilestoneFlag",
                    "CN-4",
                    "3.0",
                    "3.0 is not in field mapping or valid value set",
                ),
                _create_value_identifer(
                    "333",
                    "MilestoneFlag",
                    "CN-5",
                    "333",
                    "333 is not in field mapping or valid value set",
                ),
            ],
            invalid_values,
        )
        self.assertTrue(dropped_rows)
        for i in range(0, self.input_df.shape[0]):
            self.assertTrue(
                dropped_rows[i][data_processor.ROW_KEY].equals(self.input_df.loc[i])
            )
            self.assertEqual(
                dropped_rows[i][data_processor.MISSING_FIELDS_KEY], ["MilestoneFlag"]
            )
        # Should drop all rows since all values are unmappable.
        self.assertTrue(output_df.empty)

    def test_handleRequired_missingColumn(self):
        self.input_df = FAKE_DATA.copy()

        self.input_df.drop(columns=["MilestoneFlag"], inplace=True)

        output_df, invalid_values, dropped_rows = self.processor.process(self.input_df)

        # Should drop all rows since MilestoneFlag is missing.
        self.assertTrue(output_df.empty)
        self.assertFalse(invalid_values)
        for i in range(self.input_df.shape[0]):
            self.assertTrue(
                dropped_rows[i][data_processor.ROW_KEY].equals(self.input_df.loc[i])
            )
            self.assertEqual(
                dropped_rows[i][data_processor.MISSING_FIELDS_KEY], ["MilestoneFlag"]
            )

    def test_picklist_validData(self):
        valid_df = FAKE_DATA.copy()

        valid_df["MilestoneFlag"] = [
            "almost_Intake",
            "two years",
            "Exit",
            "Midpoint",
            "Midpoint",
        ]
        valid_df["CategoriesIdentifyWith"] = [[3], ["5"], ["2"], ["LGBTQ"], None]
        output_df, invalid_values, dropped_rows = self.processor.process(valid_df)
        self.assertFalse(invalid_values)
        self.assertFalse(dropped_rows)

    def test_multipicklist_validData(self):
        self.input_df["CategoriesIdentifyWith"] = [
            ["Immigrant", "English as a Second Language"],
            ["LGBT", "not_a_match"],
            ["not_a_match", "immigrant", "4", "5"],
            [],
            None,
        ]
        output_df, invalid_values, dropped_rows = self.processor.process(self.input_df)
        self.assertFalse(invalid_values)
        self.assertFalse(dropped_rows)

    def test_multipicklist_handleUnmappableData(self):
        self.input_df = FAKE_DATA.copy()

        self.input_df["CategoriesIdentifyWith"] = [
            ["not_a_match", "not_a_match"],
            ["aaa"],
            [None],
            [float("NaN")],
            None,
        ]
        output_df, invalid_values, dropped_rows = self.processor.process(self.input_df)
        self.assertEqual(
            [
                _create_value_identifer(
                    ["aaa"],
                    "CategoriesIdentifyWith",
                    "CN-2",
                    "TwoYears",
                    "aaa is not in field mapping or valid value set",
                )
            ],
            invalid_values,
        )
        self.assertFalse(dropped_rows)

    def test_handle_columns_not_in_spec(self):
        valid_df = FAKE_DATA.copy()

        valid_df["RandomColumn"] = ["rand", "rand", "rand", "rand", "rand"]

        output_df, invalid_values, dropped_rows = self.processor.process(valid_df)

        self.assertTrue("RandomColumn" in output_df.columns)

        self.assertEqual(
            ["rand", "rand", "rand", "rand", "rand"], output_df["RandomColumn"].tolist()
        )

    def test_drop_duplicates(self):
        input_dataframe = pd.DataFrame(
            data={
                "Date": ["2020-07-28", "2020-05-10", "2020-03-10", "2020-03-10"],
                "CaseNumber": [
                    "CASEID-000001",
                    "CASEID-000003",
                    "CASEID-xyzxyz",
                    "CASEID-xyzxyz",
                ],
                "MilestoneFlag": ["SixtyDays", "SixtyDays", "Intake", "Intake"],
                "MemberOrganization": ["abc", "abc", "abc", "abc"],
            }
        )

        expected_deduped_dataframe = pd.DataFrame(
            data={
                "Date": ["2020-07-28", "2020-05-10"],
                "CaseNumber": ["CASEID-000001", "CASEID-000003"],
                "MilestoneFlag": ["SixtyDays", "SixtyDays"],
                "MemberOrganization": ["abc", "abc"],
            }
        )

        expected_dropped_records = pd.DataFrame(
            data={
                "Date": ["2020-03-10"],
                "CaseNumber": ["CASEID-xyzxyz"],
                "MilestoneFlag": ["Intake"],
                "MemberOrganization": ["abc"],
            }
        )

        dataset_deduped, dropped_records = self.processor._drop_duplicates(
            input_dataframe
        )

        pd.util.testing.assert_frame_equal(expected_deduped_dataframe, dataset_deduped)
        pd.util.testing.assert_frame_equal(expected_dropped_records, dropped_records)


if __name__ == "__main__":
    unittest.main()
