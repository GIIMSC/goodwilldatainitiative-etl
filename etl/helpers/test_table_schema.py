import unittest
import tableschema

from etl.helpers import table_schema

TEST_SCHEMA1: tableschema.Schema = tableschema.Schema(
    {
        "fields": [{"name": "field1"}, {"name": "field2", "milestones": [0]}],
        "column_based_milestone_names": ["Intake", "Exit", "NinetyDays"],
    }
)

TEST_SCHEMA2: tableschema.Schema = tableschema.Schema(
    {
        "fields": [
            {"name": "field1"},
            {"name": "field2", "milestones": [0]},
            {"name": "field3", "milestones": [0, 1]},
            {
                "name": "field4",
                "milestones": [2],
                "custom_milestone_field_names": {"2": "actual_field4"},
            },
            {
                "name": "field5",
                "type": "integer",
                "constraints": {"minimum": 1, "maximum": 2},
                "enum_mapping": {"first_value": 1, "second_value": 2},
            },
        ],
        "column_based_milestone_names": ["Intake", "Exit", "NinetyDays"],
    }
)


class TableSchemaTest(unittest.TestCase):
    def test_get_column_format_fields_admin(self):
        _, actual_field_names_admin = table_schema.get_column_format_fields(
            TEST_SCHEMA1
        )

        expected_field_names_admin = ["field1"]
        self.assertEqual(expected_field_names_admin, actual_field_names_admin)

    def test_get_column_format_fields_single_milestone(self):
        actual_field_names_by_milestone, _ = table_schema.get_column_format_fields(
            TEST_SCHEMA1
        )

        expected_field_names_by_milestone_intake = {"Intakefield2": "field2"}
        self.assertDictEqual(
            expected_field_names_by_milestone_intake,
            actual_field_names_by_milestone["Intake"],
        )

    def test_get_column_format_fields_multiple_milestones(self):
        actual_field_names_by_milestone, _ = table_schema.get_column_format_fields(
            TEST_SCHEMA2
        )

        expected_field_names_by_milestone_intake = {
            "Intakefield2": "field2",
            "Intakefield3": "field3",
        }
        self.assertDictEqual(
            expected_field_names_by_milestone_intake,
            actual_field_names_by_milestone["Intake"],
        )

        expected_field_names_by_milestone_exit = {"Exitfield3": "field3"}
        self.assertDictEqual(
            expected_field_names_by_milestone_exit,
            actual_field_names_by_milestone["Exit"],
        )

    def test_get_column_format_fields_custom_milestone_name(self):
        actual_field_names_by_milestone, _ = table_schema.get_column_format_fields(
            TEST_SCHEMA2
        )

        expected_field_names_by_milestone_NinetyDays = {"actual_field4": "field4"}
        self.assertDictEqual(
            expected_field_names_by_milestone_NinetyDays,
            actual_field_names_by_milestone["NinetyDays"],
        )

    def test_validate_valid_schema(self):
        self.assertTrue(table_schema.validate_schema(TEST_SCHEMA2))

    def test_validate_invalid_milestone(self):
        bad_schema = tableschema.Schema(TEST_SCHEMA2.descriptor)
        bad_schema.descriptor["fields"][4]["milestones"] = [10]
        bad_schema.commit()
        self.assertFalse(table_schema.validate_schema(bad_schema))

    def test_validate_invalid_milestone_name(self):
        bad_schema = tableschema.Schema(TEST_SCHEMA2.descriptor)
        bad_schema.descriptor["fields"][4]["custom_milestone_field_names"] = {
            "10": "bad_milestone_name"
        }
        bad_schema.commit()
        self.assertFalse(table_schema.validate_schema(bad_schema))

    def test_validate_numeric_enum_missing_value(self):
        bad_schema = tableschema.Schema(TEST_SCHEMA2.descriptor)
        bad_schema.descriptor["fields"][4]["constraints"]["maximum"] = 3
        bad_schema.commit()
        self.assertFalse(table_schema.validate_schema(bad_schema))

    def test_validate_numeric_enum_not_int(self):
        bad_schema = tableschema.Schema(TEST_SCHEMA2.descriptor)
        bad_schema.descriptor["fields"][4]["type"] = "string"
        bad_schema.commit()
        self.assertFalse(table_schema.validate_schema(bad_schema))


if __name__ == "__main__":
    unittest.main()
