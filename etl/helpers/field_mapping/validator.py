from tableschema import Schema, Field
import great_expectations as ge
from typing import Dict, List

from etl.helpers.common import ge_results_to_failure_map
from etl.helpers.field_mapping import common
from etl.helpers.field_mapping.common import FieldMapping, FieldMappings


class FieldMappingValidator:
    def __init__(self, table_schema: Schema):
        self.table_schema: Schema = table_schema

    def _get_valid_mappings_for_field(self, field: Field) -> List[str]:
        if field.type == "string":
            valid_mappings = field.constraints["enum"]
        elif field.type == "integer":
            valid_mappings = list(field.descriptor["enum_mapping"].keys())
        elif field.type == "boolean":
            valid_mappings = (
                field.descriptor["trueValues"] + field.descriptor["falseValues"]
            )

        return valid_mappings + [None]

    def _get_expectations(
        self, field_mapping: FieldMapping, field_name: str
    ) -> ge.dataset.Dataset:
        field_mapping_ge = ge.from_pandas(field_mapping.get_field_mapping_df())
        field_mapping_ge.set_default_expectation_argument("result_format", "COMPLETE")

        # Shape
        shape_expectation = field_mapping_ge.expect_table_columns_to_match_ordered_list(
            common.COLUMN_NAMES
        )

        # If the shape isn't correct then the following expectations will raise exceptions
        if not shape_expectation["success"]:
            return field_mapping_ge

        field = self.table_schema.get_field(field_name)
        # Check that there aren't multiple mappings for the same input value
        field_mapping_ge.expect_column_values_to_be_unique(common.INPUT_COLUMN_NAME)

        # Check that the mapped values are all part of the available options (blank value is also valid)
        valid_mapped_values = self._get_valid_mappings_for_field(field) + [""]
        field_mapping_ge.expect_column_values_to_be_in_set(
            common.OUTPUT_COLUMN_NAME, valid_mapped_values
        )

        # Check that the approved column values are all "Yes", "No", or blank (Assumed to be no)
        field_mapping_ge.expect_column_values_to_be_in_set(
            common.APPROVED_COLUMN_NAME, common.VALID_APPROVED_VALUES + ["None"]
        )

        return field_mapping_ge

    def validate_multiple(self, field_mappings: FieldMappings) -> Dict[str, Dict]:
        """Returns map of field_name -> failures. If map is empty, the field mappings are valid."""
        return ge_results_to_failure_map(
            {
                field_name: self._get_expectations(field_mapping, field_name).validate()
                for field_name, field_mapping in field_mappings.items()
            }
        )


class FieldMappingApprovalValidator:
    def _get_expectations(self, field_mapping: FieldMapping) -> ge.dataset.Dataset:
        field_mapping_ge = ge.from_pandas(field_mapping.get_field_mapping_df())
        field_mapping_ge.set_default_expectation_argument("result_format", "COMPLETE")

        # Check that the approved column values are all "Yes"
        field_mapping_ge.expect_column_values_to_be_in_set(
            common.APPROVED_COLUMN_NAME, [common.APPROVED]
        )

        return field_mapping_ge

    def validate_multiple(self, field_mappings: FieldMappings) -> Dict[str, Dict]:
        """Returns map of field_name -> failures. If map is empty, the field mapping approvals are valid."""
        return ge_results_to_failure_map(
            {
                field_name: self._get_expectations(field_mapping).validate()
                for field_name, field_mapping in field_mappings.items()
            }
        )
