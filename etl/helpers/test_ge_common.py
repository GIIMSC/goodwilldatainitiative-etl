import logging
import great_expectations as ge
import pandas as pd
import unittest

from etl.helpers import common
from etl.helpers.dataset_shape import ShapePandasDataset

"""Unit tests for Great Expectations common helpers.

Run with `python -m etl.helpers.test_ge_common`.
"""

TEST_DATA = pd.DataFrame(
    data={"column_one": ["value1", "value1"], "column_two": ["row1_col2", "row2_col2"]}
)


class CommonTest(unittest.TestCase):
    def test_failure_map_column_ordered_list(self):
        dataset_ge = ge.from_pandas(TEST_DATA)
        dataset_ge.set_default_expectation_argument("result_format", "COMPLETE")
        dataset_ge.expect_table_columns_to_match_ordered_list(
            ["column_one", "other_column"]
        )

        failure_map = common.ge_results_to_failure_map(
            {"dataset": dataset_ge.validate()}
        )

        self.assertEqual(
            {
                "dataset": {
                    common.EXPECT_COLUMNS_MATCH_KEY: {
                        common.EXPECTED_ORDERED_LIST_KEY: [
                            "column_one",
                            "other_column",
                        ],
                        common.FAILED_VALUES_KEY: ["column_two"],
                    }
                }
            },
            failure_map,
        )

    def test_failure_map_values_in_set(self):
        dataset_ge = ge.from_pandas(TEST_DATA)
        dataset_ge.set_default_expectation_argument("result_format", "COMPLETE")
        dataset_ge.expect_column_values_to_be_in_set(
            "column_two", set(["invalid_value", "row1_col2"])
        )

        failure_map = common.ge_results_to_failure_map(
            {"dataset": dataset_ge.validate()}
        )

        self.assertEqual(
            {
                "dataset": {
                    common.EXPECT_VALUES_IN_SET_KEY: {
                        common.COLUMN_NAME_KEY: "column_two",
                        common.FAILED_VALUES_KEY: ["row2_col2"],
                    }
                }
            },
            failure_map,
        )

    def test_failure_map_values_unique(self):
        dataset_ge = ge.from_pandas(TEST_DATA)
        dataset_ge.set_default_expectation_argument("result_format", "COMPLETE")
        dataset_ge.expect_column_values_to_be_unique("column_one")

        failure_map = common.ge_results_to_failure_map(
            {"dataset": dataset_ge.validate()}
        )

        self.assertEqual(
            {
                "dataset": {
                    common.EXPECT_VALUES_UNIQUE_KEY: {
                        common.COLUMN_NAME_KEY: "column_one",
                        common.FAILED_VALUES_KEY: ["value1"],
                    }
                }
            },
            failure_map,
        )

    def test_failure_map_values(self):
        dataset_ge = ge.from_pandas(TEST_DATA, dataset_class=ShapePandasDataset)
        dataset_ge.set_default_expectation_argument("result_format", "COMPLETE")
        dataset_ge.expect_table_columns_to_be_in_set(
            set(["column_one", "other_column"])
        )

        failure_map = common.ge_results_to_failure_map(
            {"dataset": dataset_ge.validate()}
        )

        self.assertEqual(
            {
                "dataset": {
                    common.EXPECT_COLUMNS_IN_SET_KEY: {
                        common.FAILED_VALUES_KEY: ["column_two"]
                    }
                }
            },
            failure_map,
        )


if __name__ == "__main__":
    unittest.main()
