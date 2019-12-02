import unittest
import pandas as pd

from etl.helpers.field_mapping.test_metadata import TEST_SCHEMA
from etl.helpers.field_mapping.generator import FieldMappingGenerator


class FieldMappingGeneratorTest(unittest.TestCase):
    def test_generate_field_mapping_enum_value_is_null_should_not_have_mapping(self):
        field_mapping_generator = FieldMappingGenerator(TEST_SCHEMA)
        dataset = pd.DataFrame(data={"field1": [None]})

        actual_field_mappings = field_mapping_generator.generate_mappings_from_dataset(
            dataset
        )

        self.assertFalse("field1" in actual_field_mappings)

    def test_generate_field_mapping_enum_value_is_number_should_not_have_mapping(self):
        field_mapping_generator = FieldMappingGenerator(TEST_SCHEMA)
        dataset = pd.DataFrame(data={"field1": [2]})

        actual_field_mappings = field_mapping_generator.generate_mappings_from_dataset(
            dataset
        )

        self.assertFalse("field1" in actual_field_mappings)

    def test_generate_field_mapping_enum_value_exactly_matches_enum_should_not_have_mapping(
        self,
    ):
        field_mapping_generator = FieldMappingGenerator(TEST_SCHEMA)
        dataset = pd.DataFrame(data={"field1": ["Hispanic/Latino ethnic origin"]})

        actual_field_mappings = field_mapping_generator.generate_mappings_from_dataset(
            dataset
        )

        self.assertFalse("field1" in actual_field_mappings)

    def test_generate_field_mapping_enum_value_matches_without_case_should_not_have_mapping(
        self,
    ):
        field_mapping_generator = FieldMappingGenerator(TEST_SCHEMA)
        dataset = pd.DataFrame(data={"field1": ["HispaNic/Latino eThnic orIGin"]})

        actual_field_mappings = field_mapping_generator.generate_mappings_from_dataset(
            dataset
        )

        self.assertFalse("field1" in actual_field_mappings)

    def test_generate_field_mapping_enum_value_multiple_instances_should_have_only_one_mapping(
        self,
    ):
        field_mapping_generator = FieldMappingGenerator(TEST_SCHEMA)
        dataset = pd.DataFrame(
            data={"field1": ["Hispanic/Latino origin", "Hispanic/Latino origin"]}
        )

        actual_field_mappings = field_mapping_generator.generate_mappings_from_dataset(
            dataset
        )

        expected_field_mapping = {
            "Hispanic/Latino origin": ("Hispanic/Latino ethnic origin", "No")
        }
        self.assertDictEqual(
            expected_field_mapping,
            actual_field_mappings["field1"].get_field_mapping_dict(),
        )

    def test_generate_field_mapping_enum_value_blank_should_not_have_mapping(self):
        field_mapping_generator = FieldMappingGenerator(TEST_SCHEMA)
        dataset = pd.DataFrame(data={"field1": [""]})

        actual_field_mappings = field_mapping_generator.generate_mappings_from_dataset(
            dataset
        )

        self.assertFalse("field1" in actual_field_mappings)

    def test_generate_field_mapping_enum_value_should_be_mapped(self):
        field_mapping_generator = FieldMappingGenerator(TEST_SCHEMA)
        dataset = pd.DataFrame(data={"field1": ["Hispanic/Latino origin"]})

        actual_field_mappings = field_mapping_generator.generate_mappings_from_dataset(
            dataset
        )

        expected_field_mapping = {
            "Hispanic/Latino origin": ("Hispanic/Latino ethnic origin", "No")
        }
        self.assertDictEqual(
            expected_field_mapping,
            actual_field_mappings["field1"].get_field_mapping_dict(),
        )

    def test_generate_field_mapping_boolean_value_is_null_should_not_have_mapping(self):
        field_mapping_generator = FieldMappingGenerator(TEST_SCHEMA)
        dataset = pd.DataFrame(data={"field3": [None]})

        actual_field_mappings = field_mapping_generator.generate_mappings_from_dataset(
            dataset
        )

        self.assertFalse("field3" in actual_field_mappings)

    def test_generate_field_mapping_boolean_value_exactly_matches_should_not_have_mapping(
        self,
    ):
        field_mapping_generator = FieldMappingGenerator(TEST_SCHEMA)
        dataset = pd.DataFrame(data={"field3": ["yes"]})

        actual_field_mappings = field_mapping_generator.generate_mappings_from_dataset(
            dataset
        )

        self.assertFalse("field3" in actual_field_mappings)

    def test_generate_field_mapping_boolean_value_multiple_instances_should_have_only_one_mapping(
        self,
    ):
        field_mapping_generator = FieldMappingGenerator(TEST_SCHEMA)
        dataset = pd.DataFrame(data={"field3": ["1-yes", "1-yes"]})

        actual_field_mappings = field_mapping_generator.generate_mappings_from_dataset(
            dataset
        )

        expected_field_mapping = {"1-yes": ("yes", "No")}
        self.assertDictEqual(
            expected_field_mapping,
            actual_field_mappings["field3"].get_field_mapping_dict(),
        )

    def test_generate_field_mapping_boolean_value_blank_should_not_have_mapping(self):
        field_mapping_generator = FieldMappingGenerator(TEST_SCHEMA)
        dataset = pd.DataFrame(data={"field3": [""]})

        actual_field_mappings = field_mapping_generator.generate_mappings_from_dataset(
            dataset
        )

        self.assertFalse("field3" in actual_field_mappings)

    def test_generate_field_mapping_boolean_value_should_be_mapped(self):
        field_mapping_generator = FieldMappingGenerator(TEST_SCHEMA)
        dataset = pd.DataFrame(data={"field3": ["1-yes"]})

        actual_field_mappings = field_mapping_generator.generate_mappings_from_dataset(
            dataset
        )

        expected_field_mapping = {"1-yes": ("yes", "No")}
        self.assertDictEqual(
            expected_field_mapping,
            actual_field_mappings["field3"].get_field_mapping_dict(),
        )


if __name__ == "__main__":
    unittest.main()
