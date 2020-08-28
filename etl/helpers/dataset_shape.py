"""Classes and methods to help transform and validate dataset shape.
"""
import logging
from typing import Dict, List
import pandas as pd
import great_expectations as ge
from great_expectations.dataset import PandasDataset, Dataset
from tableschema import Schema, Field

from etl.helpers import column_mapping, common, table_schema

FORCE_OVERWRITE_VALUE = "1"


class ShapePandasDataset(PandasDataset):
    @Dataset.expectation(["column_list"])
    def expect_table_columns_to_be_in_set(
        self,
        column_list,
        result_format=None,
        include_config=False,
        catch_exceptions=None,
        meta=None,
    ):
        """
        Checks if observed columns are in the set of expected columns. The
        expectations will fail if columns are not in the expected set.
        On failure, details are provided on the location of the unexpected
        column(s).
        """
        named_columns = [col for col in self.columns if "Unnamed:" not in col]

        if set(named_columns) <= set(column_list):
            return {"success": True}

        invalid_cols = sorted(list(set(named_columns) - set(column_list)))
        return {
            "success": False,
            "invalid_columns": invalid_cols,
        }

    @Dataset.expectation(["column_list"])
    def expect_named_cols(
        self,
        result_format=None,
        include_config=False,
        catch_exceptions=None,
        meta=None,
    ):

        cols = list(set(self.columns))
        columns_without_headers = [col[9:] for col in cols if "Unnamed" in col]
        if not columns_without_headers:
            return {"success": True}

        return {"success": False, "columns_without_headers": columns_without_headers}


class DatasetShapeValidator:
    """Helper for validating dataset shape."""

    def __init__(
        self,
        table_schema: Schema,
        column_mapping: column_mapping.ColumnMapping,
        row_format: bool,
    ):
        self.table_schema: Schema = table_schema
        self.column_mapping: column_mapping.ColumnMapping = column_mapping
        self.row_format: bool = row_format

    def _get_shape_expectations(self, dataset: pd.DataFrame) -> ge.dataset.Dataset:
        """
        Validates dataset shape.

        Validations:
        - Dataset columns names are a subset of mapped column names and table_schema field names
        """
        dataset_ge = ge.from_pandas(dataset, dataset_class=ShapePandasDataset)
        dataset_ge.set_default_expectation_argument("result_format", "COMPLETE")

        valid_field_names: List[str] = table_schema.get_valid_field_names(
            self.table_schema, self.row_format
        )
        valid_cols: List[str] = list(self.column_mapping.keys()) + valid_field_names

        dataset_ge.expect_table_columns_to_be_in_set(valid_cols)
        dataset_ge.expect_named_cols()

        return dataset_ge

    def validate_multiple_dataset_shape(
        self, datasets: Dict[str, pd.DataFrame]
    ) -> Dict:
        """Validates all datasets and returns map of dataset_name -> failures.
        If map is empty, then all dataset shapes are valid."""
        return common.ge_results_to_failure_map(
            {
                dataset_name: self._get_shape_expectations(dataset).validate()
                for dataset_name, dataset in datasets.items()
            }
        )


class DatasetShapeTransformer:
    def __init__(
        self,
        member_id: str,
        table_schema: Schema,
        column_mapping: column_mapping.ColumnMapping,
        row_format: bool,
        multiple_val_delimiter: str = ";",
    ):
        self.member_id: str = member_id
        self.table_schema: Schema = table_schema
        self.column_mapping: column_mapping.ColumnMapping = column_mapping
        self.row_format: bool = row_format
        self.multiple_val_delimiter: str = multiple_val_delimiter

    def _transform_column_format_to_row_format(
        self, dataset: pd.DataFrame
    ) -> pd.DataFrame:
        (
            field_names_by_milestone,
            field_names_admin,
        ) = table_schema.get_column_format_fields(self.table_schema)

        admin_columns = dataset[
            list(set(field_names_admin) & set(dataset.columns.values))
        ]

        datasets = []

        # For each milestone, move the data for that milestone into their own rows
        for milestone_name, fields_for_milestone in field_names_by_milestone.items():
            # Get the milestone-specific columns from the local Goodwills dataset
            # E.g., a dataset might have 68 columns for Intake milestone data.
            # The milestone_dataset will contain those 68 columns (but not the other 300+ columns).
            milestone_field_names = fields_for_milestone.keys()
            milestone_dataset = dataset[
                list(set(milestone_field_names) & set(dataset.columns.values))
            ]

            # Drop empty rows
            milestone_dataset = milestone_dataset.dropna(how="all")

            # If, after removing all of the empty rows, the data for the milestone is empty, go to the next one
            if milestone_dataset.empty:
                continue

            # Rename column-format field names to row-format field names
            renamed_milestone_dataset = milestone_dataset.rename(
                mapper=fields_for_milestone, axis="columns"
            )

            # Add the admin columns and the MilestoneFlag column
            milestone_dataset = pd.concat(
                [admin_columns, renamed_milestone_dataset], axis=1, sort=False
            )

            # Note that the milestone name may slightly diverge from the GII-accepted MilestoneFlag values,
            # (i.e. "MidPoint" vs "Midpoint"), but any differences should be handled by fuzzy text matching later in the pipeline.
            milestone_dataset["MilestoneFlag"] = milestone_name
            datasets.append(milestone_dataset)

        return pd.concat(datasets, ignore_index=True, sort=True)

    def _transform_shape(self, dataset: pd.DataFrame) -> pd.DataFrame:
        """
        - Renames columns according to column mapping
        - Transforms column formatted data to row formatted data if required
        """
        cols_to_drop = [
            k
            for k, v in self.column_mapping.items()
            if self.column_mapping[k] is None and k in dataset.columns
        ]
        renamed_dataset: pd.DataFrame = dataset.drop(columns=cols_to_drop).rename(
            mapper=self.column_mapping, axis="columns"
        )

        if self.row_format:
            return renamed_dataset

        # Transform column-formatted data
        return self._transform_column_format_to_row_format(renamed_dataset)

    def _transform_multiple_value_fields(self, dataset: pd.DataFrame) -> pd.DataFrame:
        """Transforms data for fields that allow multiple values into arrays."""
        dataset_column_names = dataset.columns

        fields_multiple: List[Field] = list(
            filter(
                lambda field: "allows_multiple" in field.descriptor.keys()
                and field.descriptor["allows_multiple"],
                self.table_schema.fields,
            )
        )

        fields_multiple_in_dataset: List[Field] = list(
            filter(lambda field: field.name in dataset_column_names, fields_multiple)
        )

        for field in fields_multiple_in_dataset:
            dataset[field.name] = (
                dataset[field.name]
                .str.split(self.multiple_val_delimiter)
                .apply(lambda x: [s.strip() for s in x])
            )

        return dataset

    def _strip_whitespace(self, dataset: pd.DataFrame):
        for column in dataset:
            dataset[column] = dataset[column].str.strip()

        return dataset

    def _transform_columns(self, dataset: pd.DataFrame):
        """
        Sets values for certain columns to their known/expected values.
        """
        dataset["MemberOrganization"] = self.member_id
        dataset["ForceOverWrite"] = FORCE_OVERWRITE_VALUE

        return dataset

    def transform_dataset_shape(self, dataset: pd.DataFrame) -> pd.DataFrame:
        if dataset.empty:
            return dataset
        shaped_dataset = self._transform_shape(dataset)

        shaped_dataset = shaped_dataset.fillna("").astype(str)

        shaped_dataset = self._strip_whitespace(shaped_dataset)

        shaped_dataset = self._transform_multiple_value_fields(shaped_dataset)

        return self._transform_columns(shaped_dataset)


class GatewayDatasetShapeTransformer:
    """
    Class that transforms processed datasets into a format that can be uploaded
    to Gateway. This includes a final deduplication step,
    since Gateway does not accept files with duplicate records.
    """

    def __init__(self, table_schema: Schema):
        self.table_schema: Schema = table_schema

    def _allows_multiple(self, schema, field_name):
        field = schema.get_field(field_name)
        return (
            "allows_multiple" in field.descriptor.keys()
            and field.descriptor["allows_multiple"]
        )

    def _convert_multiple_val(self, val):
        """
        Converts multiple-value fields from lists to comma separated strings.
        """
        if val is None:
            return val

        return ",".join(map(str, val))

    def _reformat_multiple_val_col(self, dataset, column_name):
        dataset[column_name] = [
            self._convert_multiple_val(l) for l in dataset[column_name]
        ]

    def _drop_old_duplicates(self, dataset):
        logging.info(f"Length of dataset *before* dedupe: {dataset.shape[0]}")

        dataset = dataset.sort_values(by=["Date"], ascending=False)
        # TODO: Wait for GII to provide subset.
        dataset = dataset.drop_duplicates(
            subset=["CaseNumber", "MilestoneFlag", "MemberOrganization"]
        ).reset_index(drop=True)

        logging.info(f"Length of dataset *after* dedupe: {dataset.shape[0]}")

        return dataset

    def transform_dataset_shape(self, dataset: pd.DataFrame) -> pd.DataFrame:
        if dataset.empty:
            return dataset

        schema_columns = set(dataset.columns) & set(self.table_schema.field_names)
        multiple_val_schema_cols = [
            col
            for col in schema_columns
            if self._allows_multiple(self.table_schema, col)
        ]

        for column_name in multiple_val_schema_cols:
            self._reformat_multiple_val_col(dataset, column_name)

        dataset = self._drop_old_duplicates(dataset)

        return dataset
