EXPECT_COLUMNS_IN_SET_KEY = "expect_table_columns_to_be_in_set"
EXPECT_COLUMNS_MATCH_KEY = "expect_table_columns_to_match_ordered_list"
EXPECT_VALUES_IN_SET_KEY = "expect_column_values_to_be_in_set"
EXPECT_VALUES_UNIQUE_KEY = "expect_column_values_to_be_unique"

COLUMN_NAME_KEY = "column_name"
FAILED_VALUES_KEY = "failed_vals"
EXPECTED_ORDERED_LIST_KEY = "expected_list"


def extract_failures_from_ge_result(ge_result):
    """Takes Great Expectation result and outputs a
    map of great_expectation type key to dict of failure info.

    Failure info always includes list of invalid values.
    For column value checks, also includes columns for invalid values.
    For exact match list checks, includes expected ordered list.
    """
    if ge_result["success"]:
        return {}

    failures: Dict[str, Dict] = {}
    for result in ge_result["results"]:
        if not result["success"]:
            expectation_type = result["expectation_config"]["expectation_type"]
            if expectation_type is EXPECT_COLUMNS_IN_SET_KEY:
                failures[expectation_type] = {
                    FAILED_VALUES_KEY: result["invalid_columns"]
                }
            elif expectation_type is "expect_named_cols":
                failures[expectation_type] = {
                    FAILED_VALUES_KEY: result["columns_without_headers"]
                }
            elif expectation_type is EXPECT_COLUMNS_MATCH_KEY:
                failures[expectation_type] = {
                    FAILED_VALUES_KEY: [
                        x["Found"] for x in result["details"]["mismatched"]
                    ],
                    EXPECTED_ORDERED_LIST_KEY: result["expectation_config"]["kwargs"][
                        "column_list"
                    ],
                }
            elif expectation_type is EXPECT_VALUES_IN_SET_KEY:
                failures[expectation_type] = {
                    FAILED_VALUES_KEY: result["result"]["unexpected_list"],
                    COLUMN_NAME_KEY: result["expectation_config"]["kwargs"]["column"],
                }
            elif expectation_type is EXPECT_VALUES_UNIQUE_KEY:
                failures[expectation_type] = {
                    FAILED_VALUES_KEY: list(set(result["result"]["unexpected_list"])),
                    COLUMN_NAME_KEY: result["expectation_config"]["kwargs"]["column"],
                }
            else:
                failures[expectation_type] = result

    return failures


def ge_results_to_failure_map(validation_results):
    """Takes a map of name to Great Expectation result and
    outputs a map from name to failures for the corresponding result."""
    failure_map: Dict[str, Dict] = {}
    for name, ge_result in validation_results.items():
        if not ge_result["success"]:
            failures = extract_failures_from_ge_result(ge_result)
            failure_map[name] = failures

    return failure_map
