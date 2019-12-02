import unittest

from etl.helpers.field_mapping.test_metadata import TEST_SCHEMA
from etl.helpers.field_mapping.resolver import FieldMappingResolver
from etl.helpers.field_mapping.common import FieldMapping


class FieldMappingResolverTest(unittest.TestCase):
    def test_resolve_mappings_do_not_overwrite(self):
        source_mappings = {
            "field1": FieldMapping.from_dict({"input1": ("original", "Yes")})
        }
        new_mappings = {"field1": FieldMapping.from_dict({"input1": ("new", "No")})}

        actual_resolved_mappings = FieldMappingResolver.resolve_mappings(
            new_mappings,
            source_mappings,
            overwrite=False,
            remove_unapproved_source_mappings=False,
        )

        actual_resolved_mappings_dict = {
            field_name: field_mapping.get_field_mapping_dict()
            for field_name, field_mapping in actual_resolved_mappings.items()
        }

        expected_resolved_mappings = {"field1": {"input1": ("original", "Yes")}}

        self.assertDictEqual(expected_resolved_mappings, actual_resolved_mappings_dict)

    def test_resolve_mappings_overwrite(self):
        source_mappings = {
            "field1": FieldMapping.from_dict({"input1": ("original", "Yes")})
        }
        new_mappings = {"field1": FieldMapping.from_dict({"input1": ("new", "No")})}

        actual_resolved_mappings = FieldMappingResolver.resolve_mappings(
            new_mappings,
            source_mappings,
            overwrite=True,
            remove_unapproved_source_mappings=False,
        )

        actual_resolved_mappings_dict = {
            field_name: field_mapping.get_field_mapping_dict()
            for field_name, field_mapping in actual_resolved_mappings.items()
        }

        expected_resolved_mappings = {"field1": {"input1": ("new", "No")}}

        self.assertDictEqual(expected_resolved_mappings, actual_resolved_mappings_dict)

    def test_resolve_mappings_do_not_overwrite_field_not_in_source(self):
        source_mappings = {}
        new_mappings = {"field1": FieldMapping.from_dict({"input1": ("new", "No")})}

        actual_resolved_mappings = FieldMappingResolver.resolve_mappings(
            new_mappings,
            source_mappings,
            overwrite=False,
            remove_unapproved_source_mappings=False,
        )

        actual_resolved_mappings_dict = {
            field_name: field_mapping.get_field_mapping_dict()
            for field_name, field_mapping in actual_resolved_mappings.items()
        }

        expected_resolved_mappings = {"field1": {"input1": ("new", "No")}}

        self.assertDictEqual(expected_resolved_mappings, actual_resolved_mappings_dict)

    def test_resolve_mappings_do_not_overwrite_field_not_in_new(self):
        source_mappings = {
            "field1": FieldMapping.from_dict({"input1": ("original", "Yes")})
        }
        new_mappings = {}

        actual_resolved_mappings = FieldMappingResolver.resolve_mappings(
            new_mappings,
            source_mappings,
            overwrite=False,
            remove_unapproved_source_mappings=False,
        )

        actual_resolved_mappings_dict = {
            field_name: field_mapping.get_field_mapping_dict()
            for field_name, field_mapping in actual_resolved_mappings.items()
        }

        expected_resolved_mappings = {"field1": {"input1": ("original", "Yes")}}

        self.assertDictEqual(expected_resolved_mappings, actual_resolved_mappings_dict)

    def test_resolve_mappings_overwrite_field_not_in_source(self):
        source_mappings = {}
        new_mappings = {"field1": FieldMapping.from_dict({"input1": ("new", "No")})}

        actual_resolved_mappings = FieldMappingResolver.resolve_mappings(
            new_mappings,
            source_mappings,
            overwrite=True,
            remove_unapproved_source_mappings=False,
        )

        actual_resolved_mappings_dict = {
            field_name: field_mapping.get_field_mapping_dict()
            for field_name, field_mapping in actual_resolved_mappings.items()
        }

        expected_resolved_mappings = {"field1": {"input1": ("new", "No")}}

        self.assertDictEqual(expected_resolved_mappings, actual_resolved_mappings_dict)

    def test_resolve_mappings_overwrite_field_not_in_new(self):
        source_mappings = {
            "field1": FieldMapping.from_dict({"input1": ("original", "Yes")})
        }
        new_mappings = {}

        actual_resolved_mappings = FieldMappingResolver.resolve_mappings(
            new_mappings,
            source_mappings,
            overwrite=True,
            remove_unapproved_source_mappings=False,
        )

        actual_resolved_mappings_dict = {
            field_name: field_mapping.get_field_mapping_dict()
            for field_name, field_mapping in actual_resolved_mappings.items()
        }

        expected_resolved_mappings = {"field1": {"input1": ("original", "Yes")}}

        self.assertDictEqual(expected_resolved_mappings, actual_resolved_mappings_dict)

    def test_resolve_mappings_do_not_remove_unapproved_source_mappings(self):
        source_mappings = {
            "field1": FieldMapping.from_dict({"input1": ("original", "No")})
        }
        new_mappings = {}

        actual_resolved_mappings = FieldMappingResolver.resolve_mappings(
            new_mappings, source_mappings, remove_unapproved_source_mappings=False
        )

        actual_resolved_mappings_dict = {
            field_name: field_mapping.get_field_mapping_dict()
            for field_name, field_mapping in actual_resolved_mappings.items()
        }

        expected_resolved_mappings = {"field1": {"input1": ("original", "No")}}

        self.assertDictEqual(expected_resolved_mappings, actual_resolved_mappings_dict)

    def test_resolve_mappings_remove_unapproved_source_mappings(self):
        source_mappings = {
            "field1": FieldMapping.from_dict({"input1": ("original", "No")})
        }
        new_mappings = {}

        actual_resolved_mappings = FieldMappingResolver.resolve_mappings(
            new_mappings, source_mappings, remove_unapproved_source_mappings=True
        )

        actual_resolved_mappings_dict = {
            field_name: field_mapping.get_field_mapping_dict()
            for field_name, field_mapping in actual_resolved_mappings.items()
        }

        expected_resolved_mappings = {}

        self.assertDictEqual(expected_resolved_mappings, actual_resolved_mappings_dict)


if __name__ == "__main__":
    unittest.main()
