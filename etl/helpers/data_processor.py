import logging
import numpy as np
import pandas as pd
import re
from tableschema import exceptions, Schema, Field
import us

import sqlalchemy

from etl.helpers.field_mapping.common import FieldMapping, FieldMappings

# Value to represent blank/empty cells.
BLANK_VALUE = None

# Keys for data processing metadata.
NUM_ROWS_TO_UPLOAD_KEY = "num_rows_to_upload"
DROPPED_ROWS_KEY = "dropped_rows"
DROPPED_VALUES_KEY = "dropped_values"

# Keys used for value identifiers and reporting invalid data.
CASE_NUMBER_KEY = "case_number"
FIELD_NAME_KEY = "field_name"
MILESTONE_FLAG_KEY = "milestone_flag"
ORIGINAL_VALUE_KEY = "original_value"
INVALID_REASON_KEY = "invalid_reasons"

MISSING_FIELDS_KEY = "missing_fields"
DUPLICATE_ROWS_KEY = "duplicate_rows"
ROW_KEY = "row"


def is_int(s: str) -> bool:
    """Returns True iff the field can be interpreted as an int.

    Valid value examples: 3, 3.0, "3", "3.0"
    """
    try:
        num = float(s)
    except (ValueError, TypeError) as e:
        return False

    # Handle NaN
    if num != num:
        return False

    return num == round(num)


def is_blank(val):
    """Returns True iff the field appears to be missing.

    Empty string, None, and NaN are treated as missing values. Missing values are not
    invalid (unless the field is required), and will not be reported/logged.
    """
    return val is None or val != val or (isinstance(val, str) and val.strip() == "")


class DataProcessor:
    def __init__(self, field_mappings, table_schema: Schema):
        self.field_mappings = (
            field_mappings  # Field mappings for enum and boolean values.
        )
        self.table_schema: Schema = table_schema  # Schema of fields present in dataframe.
        self.dropped_rows = []
        self.invalid_values = []

    def _report_invalid_value(self, value_identifier, reason, suppress_invalid):
        """Reports invalid value.

        Saves the reason that the value is invalid as part of the value identifier.

        Adds tuple of original input value (pre-transformation), field name,
        and identifiers (case number and milestone flag) to invalid_values
        for reporting.

        If suppress_invalid is True, don't log or append, just store the reason
        and return the tuple.

        Returns tuple (BLANK_VALUE, False) to indicate input value is invalid.
        """

        # Only save the first reason that the value is invalid.
        if INVALID_REASON_KEY not in value_identifier and reason is not None:
            value_identifier[INVALID_REASON_KEY] = reason

        if not suppress_invalid:
            case_number = value_identifier[CASE_NUMBER_KEY]
            field_name = value_identifier[FIELD_NAME_KEY]
            milestone_flag = value_identifier[MILESTONE_FLAG_KEY]
            original_value = value_identifier[ORIGINAL_VALUE_KEY]

            logging.error(
                "(Case Number: %s, Milestone: %s) Invalid value for %s: %s. Type: %s. Reason: %s",
                case_number,
                milestone_flag,
                field_name,
                str(original_value),
                type(original_value),
                reason,
            )
            self.invalid_values.append(value_identifier)
        return (BLANK_VALUE, False)

    def _transform_enum(self, val, field, value_identifier, suppress_invalid=False):
        """Transforms enum based on field mapping.

        This function handles booleans, string enums, and integer enums.

        Returns tuple of transformed value and bool indicating if the input value is valid.
        If input value is not valid, returns BLANK_VALUE instead of transformed value.
        """
        data_type = field.type

        # Enum options are stored differently based on field type.
        if data_type == "boolean":
            enum_options = (
                field.descriptor["trueValues"] + field.descriptor["falseValues"]
            )
            # If the value is "1.0" or "2.0", make sure the decimals and 0 are stripped.
            if is_int(val):
                val = str(int(float(val)))
        elif data_type == "integer":
            # If the field is an integer enum and the value can be intepreted as an integer, return its integer value.
            if is_int(val):
                return (int(float(val)), True)
            enum_options = field.descriptor["enum_mapping"]
        elif data_type == "string":
            val = str(val)
            enum_options = field.constraints["enum"]

        if field.name in self.field_mappings:
            mapping = self.field_mappings[field.name].get_field_mapping_dict()
        else:
            mapping = {}

        if val in mapping:
            # Ignore the approval state, not needed here
            mapped_val, _ = mapping[val]

            # Return BLANK_VALUE if mapped value is empty.
            if is_blank(mapped_val):
                return (BLANK_VALUE, True)

            # For integer enums, the enum options are a dict mapping from string
            # values to integer values, so we use this dict to transform to int.
            return (
                (mapped_val, True)
                if data_type != "integer"
                else (enum_options[mapped_val], True)
            )
        elif data_type == "integer":
            case_insensitive_enum_options = {
                option.lower(): num for option, num in enum_options.items()
            }
            if val.lower() in case_insensitive_enum_options:
                enum_index = case_insensitive_enum_options[val.lower()]
                return (enum_index, True)
        else:
            case_insensitive_enum_options = [option.lower() for option in enum_options]
            if val.lower() in case_insensitive_enum_options:
                idx = case_insensitive_enum_options.index(val.lower())
                return (enum_options[idx], True)

        invalid_reason = f"{val} is not in field mapping or valid value set"

        # If field is boolean, include list of valid boolean values.
        if data_type == "boolean":
            invalid_reason += f" ({str(enum_options)})"

        return self._report_invalid_value(
            value_identifier, invalid_reason, suppress_invalid
        )

    def _transform_soc(self, val, field, value_identifier, suppress_invalid=False):
        """Remove numbers after decimal point from SOC value.

        Doesn't check regex here, since pattern matching is done in cast_val.

        Returns tuple of SOC and bool indicating if the input value is valid.
        If input value is not valid, returns BLANK_VALUE instead of SOC.
        """
        if isinstance(val, str):
            return (val.strip().split(".")[0], True)
        else:
            invalid_reason = f"{val} is not a string"

            return self._report_invalid_value(
                value_identifier, invalid_reason, suppress_invalid
            )

    def _transform_state(self, val, field, value_identifier, suppress_invalid=False):
        """Transforms state to two-letter abbreviation.

        The states lookup can handle full state names, abbreviations, and some misspellings.
        If no state is found, the value is considered invalid.

        Returns tuple of state abbreviation and bool indicating if the input value is valid.
        If input value is not valid, returns BLANK_VALUE instead of state abbreviation.
        """
        s = us.states.lookup(str(val).strip())
        return (
            (s.abbr, True)
            if s
            else self._report_invalid_value(
                value_identifier, f"{val} is not a valid state", suppress_invalid
            )
        )

    def _transform_int(self, val, field, value_identifier, suppress_invalid=False):
        """Transforms integer.

        Returns tuple of int value and bool indicating if the input value is valid.
        If input value is not valid, returns BLANK_VALUE instead of int value.
        """
        if is_int(val):
            return (int(float(val)), True)
        else:
            return self._report_invalid_value(
                value_identifier, f"{val} is not an integer", suppress_invalid
            )

    def _cast_val(self, value, field, value_identifier, suppress_invalid=False):
        """Cast value to proper type using schema.

        Returns tuple of casted value and bool indicating if the input value is valid.
        If input value is not valid, returns BLANK_VALUE instead of casted value.
        """
        try:
            return (field.cast_value(value, constraints=True), True)
        except exceptions.CastError as e:
            return self._report_invalid_value(
                value_identifier,
                self._parse_cast_error(e, value, field),
                suppress_invalid,
            )

    def _parse_cast_error(self, e, value, field):
        error_message = str(e)
        if "maximum" in error_message or "minimum" in error_message:
            if "maximum" in field.constraints and "minimum" in field.constraints:
                return "{} must be within the range [{}, {}]".format(
                    field.name,
                    str(field.constraints["minimum"]),
                    str(field.constraints["maximum"]),
                )
            elif "maximum" in field.constraints:
                return "{} must be less than or equal to {}".format(
                    field.name, str(field.constraints["maximum"])
                )
            elif "minimum" in field.constraints:
                return "{} must be greater than or equal to {}".format(
                    field.name, str(field.constraints["minimum"])
                )
        elif '"date"' in error_message:
            return f"{str(value)} is not a valid date"
        elif field.name == "SOC":
            return "SOC should be in the format ##-####"

        return f"{str(value)} is not a valid {field.type}"

    def _apply_multiple(self, function, values, field, value_identifier):
        """Applies a function to each value in a multiple value cell.

        The cell is considered invalid if the input is not a list, or any value in the list is invalid.

        Returns the list of values with function applied.
        If list is empty or input is not valid, returns BLANK_VALUE.
        """
        if np.isscalar(values):
            return self._report_invalid_value(
                value_identifier, f"{str(values)} is not a list", False
            )[0]

        transformed_tuples = [
            (BLANK_VALUE, True)
            if is_blank(value)
            else function(value, field, value_identifier, suppress_invalid=True)
            for value in values
        ]

        # If a single value in the cell is invalid, then drop the entire cell.
        if not all(map(lambda t: t[1], transformed_tuples)):
            return self._report_invalid_value(value_identifier, None, False)[0]

        # Get transformed values and filter out blank values.
        transformed_vals = map(lambda t: t[0], transformed_tuples)
        non_missing_vals = list(
            filter(lambda val: val != BLANK_VALUE, transformed_vals)
        )

        # If the list is empty, return BLANK_VALUE instead.
        return non_missing_vals if non_missing_vals else BLANK_VALUE

    def _apply_function(self, data_column, function, field, value_identifiers):
        """Applies a function to a given field/column in the dataframe.

        Returns the column with the function applied and invalid values replaced with BLANK_VALUE.
        """
        data_iterator = pd.concat([data_column, value_identifiers], axis=1).itertuples()

        allows_multiple = (
            "allows_multiple" in field.descriptor.keys()
            and field.descriptor["allows_multiple"]
        )

        if allows_multiple:
            return pd.Series(
                [
                    BLANK_VALUE
                    if is_blank(value)
                    else self._apply_multiple(function, value, field, value_identifier)
                    for _, value, value_identifier in data_iterator
                ],
                dtype="object",
            )

        else:
            return pd.Series(
                [
                    BLANK_VALUE
                    if is_blank(value)
                    else function(value, field, value_identifier)[0]
                    for _, value, value_identifier in data_iterator
                ],
                dtype="object",
            )

    def _create_value_identifiers(self, data, column_name):
        """Create identifiers for each value.

        Used for reporting invalid values.

        Identifiers include the case number, field name, milestone flag, and
        original value of the given column pre-transformation.
        """
        return pd.Series(
            [
                {
                    CASE_NUMBER_KEY: case_number,
                    FIELD_NAME_KEY: column_name,
                    MILESTONE_FLAG_KEY: milestone_flag,
                    ORIGINAL_VALUE_KEY: original_value,
                }
                for _, case_number, milestone_flag, original_value in data[
                    ["CaseNumber", "MilestoneFlag", column_name]
                ].itertuples()
            ],
            dtype="object",
        )

    def _process_column(self, df, column_name):
        """Transforms, validates, and casts a column in the dataframe.

        Any invalid values will be dropped during transformation/casting.
        """

        field: Field = self.table_schema.get_field(column_name)

        data_type = field.type
        is_enum: bool = (
            "enum" in field.constraints.keys()
            or "enum_mapping" in field.descriptor.keys()
        )

        value_identifiers = self._create_value_identifiers(df, column_name)

        # Transform values that need transformations.
        transform_func = None

        if is_enum or data_type == "boolean":
            transform_func = self._transform_enum
        elif column_name == "SOC":
            transform_func = self._transform_soc
        elif column_name == "State":
            transform_func = self._transform_state
        elif data_type == "integer":
            transform_func = self._transform_int

        if transform_func is not None:
            df[column_name] = self._apply_function(
                df[column_name], transform_func, field, value_identifiers
            )

        # Cast values using Schema Field.
        df[column_name] = pd.Series(
            self._apply_function(
                df[column_name], self._cast_val, field, value_identifiers
            ),
            dtype="object",
        )

    def _get_invalid_required_fields(self, row, required_fields):
        """Returns all required fields that are missing in a row."""
        return [f for f in required_fields if row[f] is BLANK_VALUE]

    def _drop_duplicates(self, dataset):
        """Returns a DataFrame with duplicate records removed and a DataFrame with the first instance of
        each duplicate record.

        GII defines 'duplicate' as records with the same CaseNumber, MilestoneFlag, and MemberOrganization.
        """
        logging.info(f"Length of dataset *before* dedupe: {dataset.shape[0]}")

        dataset_deduped = dataset.drop_duplicates(
            keep=False, subset=["CaseNumber", "MilestoneFlag", "MemberOrganization"]
        ).reset_index(drop=True)

        dropped_rows = dataset[
            dataset.duplicated(
                keep=False, subset=["CaseNumber", "MilestoneFlag", "MemberOrganization"]
            )
        ].reset_index(drop=True)

        logging.info(f"Length of dataset *after* dedupe: {dataset_deduped.shape[0]}")

        return dataset_deduped, dropped_rows

    def process(self, dataset):
        """Transforms and validates the entire Dataframe.
        Expects dataframe to contain all required columns and no columns outside of the
        schema.

        Drops cells if they don't have a valid value.
        Drops rows if they don't have a valid value for a required value.

        Returns a tuple with:
          - Transformed dataframe
          - List of tuples of invalid values
            - Each tuple includes the invalid value, the field name of the value, and row identifier.
          - List of tuples of dropped rows
            - Each tuple includes the row and the name of the required column that had the invalid/missing value.
        """

        df = dataset.copy()

        required_fields = [
            field.name for field in self.table_schema.fields if field.required
        ]

        # Process required columns.
        for column_name in required_fields:
            if column_name not in df:
                df[column_name] = None
            else:
                self._process_column(df, column_name)

        # Drop any rows where required columns are missing, and record dropped rows.
        # Note: If BLANK_VALUE is changed to not be None, this will break.
        null_rows = df[df[required_fields].isnull().any(axis=1)]
        df = df.dropna(subset=required_fields).reset_index(drop=True)
        for ind, new_row in null_rows.iterrows():
            # Get all invalid required fields for this row.
            missing_fields = self._get_invalid_required_fields(new_row, required_fields)

            # Log and record the pre-transformed row.
            row = dataset.loc[ind]
            logging.error(
                "Dropping row %d due to invalid/missing value for critical field(s):\n\t%s",
                ind,
                {
                    col: dataset.loc[ind, col] if col in dataset else None
                    for col in missing_fields
                },
            )
            self.dropped_rows.append({ROW_KEY: row, MISSING_FIELDS_KEY: missing_fields})

        # Drop duplicates, and record dropped rows.
        df, dropped_duplicate_rows = self._drop_duplicates(df)
        for ind, dropped_row in dropped_duplicate_rows.iterrows():
            logging.error(
                f"Dropping row with CaseNumber {dropped_row['CaseNumber']} due to duplicate values in the uploaded file"
            )
            self.dropped_rows.append({ROW_KEY: dropped_row, DUPLICATE_ROWS_KEY: True})

        # Process the non-required columns.
        non_required_columns = (
            set(df.columns) & set(self.table_schema.field_names)
        ) - set(required_fields)
        for column_name in non_required_columns:
            self._process_column(df, column_name)

        return df, self.invalid_values, self.dropped_rows
