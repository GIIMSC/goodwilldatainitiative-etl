import pandas as pd
from typing import Dict, List
from tableschema import Schema, Field
import fuzzywuzzy
from fuzzywuzzy import process

from etl.helpers.field_mapping.common import FieldMapping, FieldMappings, NOT_APPROVED


def is_num(s: str) -> bool:
    try:
        float(s)
    except ValueError:
        return False

    return True


class FieldMappingGenerator:
    """Provides methods to associate Pandas Dataframes with JSON-defined variables
    names.
    """

    class FieldMappingTable:
        FieldMappingDict = Dict[str, str]
        """Tracks the raw_field->validated_field mappings for a field type."""

        def __init__(self):
            self._map: self.FieldMappingDict = {}  # Maps str -> Tuple[str, int].

        def __contains__(self, _raw):
            """Implement `in` keyword for FieldMappingTable."""
            return _raw in self._map

        def insert(self, _raw: str, _mapped: str) -> None:
            """Inserts a (_raw, _validated) string mapping"""
            if not self.__contains__(_raw):
                self._map[_raw] = _mapped
                return
            mapped = self._map[_raw]
            if _mapped != mapped:
                raise RuntimeError(
                    "Raw field '{}' has already been matched with '{}', but is trying to be assigned "
                    "to '{}' as well. The fuzzy logic matcher should not be nondeterministic within "
                    "a single mapping table; is this object being used correctly?".format(
                        _raw, mapped, _mapped
                    )
                )

        def get_map(self) -> FieldMappingDict:
            return {
                input: (output, NOT_APPROVED) for input, output in self._map.items()
            }

    def __init__(self, table_schema: Schema):
        self.mapping_tables: Dict[
            str, FieldMapping
        ] = {}  # Maps str to FieldMappingTable.
        self.table_schema: Schema = table_schema

    def _get_fields_by_type(self, type: str) -> [Field]:
        """Returns fields of a provided type.
        """
        return list(filter(lambda field: field.type == type, self.table_schema.fields))

    def _get_enum_fields(self) -> [Field]:
        """Returns fields that have a provided constraint.

        For enum fields that remain as strings (String Enums), the field has "enum"
        constraint.
        For enum fields that are converted to integers (Integer Enums), the field
        has "enum_mapping" in descriptor.
        """
        return list(
            filter(
                lambda field: "enum" in field.constraints.keys()
                or "enum_mapping" in field.descriptor.keys(),
                self.table_schema.fields,
            )
        )

    def _create_enum_mapping(self, field: Field, raw_text: str) -> None:
        """Creates a fuzzy text mapping for text to a field's enum options and
        stores it in the field's mapping table.
        """

        # Enum options are stored differently based on field type.
        if field.type == "boolean":
            enum_options = (
                field.descriptor["trueValues"] + field.descriptor["falseValues"]
            )
        elif field.type == "integer":
            enum_options = field.descriptor["enum_mapping"].keys()
        elif field.type == "string":
            enum_options = field.constraints["enum"]

        # Don't map
        # - blank values
        # - numeric options
        # - values that match exactly to enum_options (this is case-insensitive)
        # - already mapped values
        if (
            raw_text == ""
            or raw_text is None
            or is_num(raw_text)
            or raw_text.lower() in [option.lower() for option in enum_options]
            or raw_text in self.mapping_tables[field.name]
        ):
            return

        mapped_text, match_score = process.extractOne(
            raw_text, enum_options, scorer=fuzzywuzzy.fuzz.token_sort_ratio
        )
        # If the best mapping does not have a match score > 50, ignore it
        if match_score > 50:
            self.mapping_tables[field.name].insert(raw_text, mapped_text)
        else:
            self.mapping_tables[field.name].insert(raw_text, None)

    def _create_enum_mapping_multiple(
        self, field: Field, data_series: pd.Series
    ) -> None:
        """Creates enum mappings for an enum field that allows multiple values."""
        for _, multiple_raw_values in data_series.iteritems():
            if multiple_raw_values is None:
                continue
            for raw_text in multiple_raw_values:
                self._create_enum_mapping(field, raw_text)

    def _create_enum_mapping_single(self, field: Field, data_series: pd.Series) -> None:
        """Creates field mappings for an enum field that only allows a single value."""
        for _, raw_text in data_series.iteritems():
            self._create_enum_mapping(field, raw_text)

    def _create_enum_mapping_dataset(self, dataset: pd.DataFrame) -> None:
        """Creates field mappings for all fields that have the enum constraint."""
        dataset_column_names = dataset.columns
        enum_fields: List[Field] = self._get_enum_fields()

        enum_fields_in_dataset: List[Field] = filter(
            lambda field: field.name in dataset_column_names, enum_fields
        )

        for field in enum_fields_in_dataset:
            self.mapping_tables[field.name] = FieldMappingGenerator.FieldMappingTable()
            allows_multiple: bool = "allows_multiple" in field.descriptor.keys() and field.descriptor[
                "allows_multiple"
            ]
            if allows_multiple:
                self._create_enum_mapping_multiple(field, dataset[field.name])
            else:
                self._create_enum_mapping_single(field, dataset[field.name])

    def _create_boolean_mapping(self, field: Field, data_series: pd.Series) -> None:
        """Creates field mappings for a boolean field."""
        for _, raw_bool in data_series.iteritems():
            self._create_enum_mapping(field, raw_bool)

    def _create_boolean_mappings(self, dataset: pd.DataFrame) -> None:
        """Creates field mappings for all fields that have the boolean type."""
        dataset_column_names = dataset.columns
        boolean_fields: List[Field] = self._get_fields_by_type("boolean")

        boolean_fields_in_dataset: List[Field] = filter(
            lambda field: field.name in dataset_column_names, boolean_fields
        )

        for field in boolean_fields_in_dataset:
            self.mapping_tables[field.name] = FieldMappingGenerator.FieldMappingTable()
            self._create_boolean_mapping(field, dataset[field.name])

    def _create_mappings(self, dataset: pd.DataFrame) -> None:
        """Creates field mappings for all enum and boolean fields."""
        self._create_enum_mapping_dataset(dataset)
        self._create_boolean_mappings(dataset)

    def generate_mappings_from_dataset(self, dataset: pd.DataFrame) -> FieldMappings:
        """Creates field mappings for applicable fields. Only returns non-empty
        mappings.
        """
        if dataset is None:
            return {}

        self._create_mappings(dataset)
        return {
            field_name: FieldMapping.from_dict(mapping_table.get_map())
            for field_name, mapping_table in self.mapping_tables.items()
            if mapping_table.get_map()
        }
