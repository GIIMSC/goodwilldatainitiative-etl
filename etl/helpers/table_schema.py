import logging
import pkg_resources
from tableschema import Schema
import typing

FieldNamesByMilestone = typing.Dict[str, typing.Dict[str, str]]
FieldNamesAdmin = typing.List[str]

MISSION_IMPACT_SCHEMA_FILE = pkg_resources.resource_filename(
    "etl.schemas", "mission_impact_table_schema.json"
)


def get_schema(schema_filename: str) -> Schema:
    return Schema(schema_filename)


def get_milestone_names(table_schema: Schema) -> typing.List[str]:
    return table_schema.descriptor["column_based_milestone_names"]


def get_column_format_fields(
    table_schema: Schema,
) -> typing.Tuple[FieldNamesByMilestone, FieldNamesAdmin]:
    """Returns a tuple
       1. Field names for column formatted data, broken down by milestone
         {
            milestone1: {
                milestone1_field_name1: row_format_field_name1,
                milestone1_field_name2: row_format_field_name2
            },
            milestone2: {...}
         }

        2. Administrative field names (no milestones):
           [
             admin_field1, admin_field2, ...
           ]
    """
    # Retrieve the values described here:
    # https://github.com/GIIMSC/goodwilldatainitiative-etl/blob/master/etl/schemas/mission_impact_table_schema.json#L2250
    milestone_names: typing.List[str] = get_milestone_names(table_schema)

    field_names_by_milestone: FieldNamesByMilestone = {}
    field_names_admin: FieldNamesAdmin = []

    # Put milestone names into a dict
    for milestone_name in milestone_names:
        field_names_by_milestone[milestone_name] = {}

    for field in table_schema.fields:
        # Some fields do not store data relating to participant milestone, e.g. "toDelete"
        # We flag these as "admin fields" and continue iteration over JSON fields.
        if "milestones" not in field.descriptor:
            field_names_admin.append(field.name)
            continue

        field_milestones: typing.List[int] = field.descriptor["milestones"]
        if "custom_milestone_field_names" not in field.descriptor:
            field_custom_milestone_field_names = {}
        else:
            field_custom_milestone_field_names: typing.Dict[
                str, str
            ] = field.descriptor["custom_milestone_field_names"]

        for milestone_number in field_milestones:
            milestone_name = milestone_names[milestone_number]
            # If the field has a custom name for a milestone, use that
            if str(milestone_number) in field_custom_milestone_field_names.keys():
                field_name_for_milestone = field_custom_milestone_field_names[
                    str(milestone_number)
                ]
            # If the field doesn't have a custom name, assume that it's milestone name is milestone name + field name
            else:
                field_name_for_milestone = milestone_name + field.name
            field_names_by_milestone[milestone_name][
                field_name_for_milestone
            ] = field.name

    return field_names_by_milestone, field_names_admin


def get_valid_field_names(table_schema: Schema, row_format: bool) -> typing.List[str]:
    if row_format:
        return table_schema.field_names

    field_names_by_milestone, field_names_admin = get_column_format_fields(table_schema)
    valid_field_names: typing.List[str] = field_names_admin
    for _, fields_for_milestone in field_names_by_milestone.items():
        valid_field_names = valid_field_names + list(fields_for_milestone.keys())

    return valid_field_names


def validate_schema(table_schema: Schema) -> bool:
    """Returns True if table_schema appears to be valid for pipeline processing.

    This will only fail if the schema itself or pipeline code are incorrect, so
    the local Goodwills need only make sure that the maintainers are aware of
    the bug.
    """
    is_valid = True
    milestone_count = len(get_milestone_names(table_schema))
    for field in table_schema.fields:

        # Check milestones.
        if "milestones" in field.descriptor:
            milestones = field.descriptor["milestones"]
            for m in milestones:
                if m not in range(milestone_count):
                    is_valid = False
                    logging.error(
                        "Schema error for field '%s': Milestone index %s out of range.",
                        field.name,
                        m,
                    )

        # Check milestone name keys.
        if "custom_milestone_field_names" in field.descriptor:
            milestone_names = field.descriptor["custom_milestone_field_names"]
            for m in milestone_names:
                if int(m) not in range(milestone_count):
                    is_valid = False
                    logging.error(
                        "Error for field '%s': Milestone index %s out of range for "
                        "custom milestone field name: '%s'",
                        field.name,
                        m,
                        milestone_names[m],
                    )

        # Check enum mappings.
        if "enum_mapping" in field.descriptor:
            if field.type != "integer":
                is_valid = False
                logging.error(
                    "Error for field '%s': Enums with numerical mappings should be of type `integer`.",
                    field.name,
                )

            if "minimum" not in field.constraints or "maximum" not in field.constraints:
                is_valid = False
                logging.error(
                    "Error for field '%s': Enums with numerical mappings should have min/max constraints.",
                    field.name,
                )
                continue  # Don't let logic blow up into a stack trace.

            mapping_values = set(field.descriptor["enum_mapping"].values())
            required_enum_values = set(
                range(field.constraints["minimum"], field.constraints["maximum"] + 1)
            )

            if mapping_values != required_enum_values:
                is_valid = False
                logging.error(
                    "Error for field '%s': Enum mapping should have value for every number between `minimum` and `maximum`. Missing values: %s",
                    field.name,
                    required_enum_values - mapping_values,
                )

    if "column_based_milestone_names" not in table_schema.descriptor:
        is_valid = False
        logging.error("Schema must include column-based milestone names.")

    return is_valid


def airflow_load_schema(**kwargs):
    return get_schema(MISSION_IMPACT_SCHEMA_FILE)
