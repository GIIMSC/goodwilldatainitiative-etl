import unittest
import pandas as pd

from etl.helpers.field_mapping import common
from etl.helpers.field_mapping.common import FieldMapping


class TestCommonMethods(unittest.TestCase):
    def test_get_field_mapping_filename(self):
        filename = common.get_field_mapping_filename("field_name", "random_dir/")

        self.assertEqual("random_dir/field_name.csv", filename)


class TestFieldMapping(unittest.TestCase):
    def test_get_field_mapping_df(self):
        field_mapping = FieldMapping.from_dict(
            {
                "sample_input": ("sample_output", "yes"),
                "sample_input2": ("sample_output2", "no"),
            }
        )

        actual_field_mapping_df = field_mapping.get_field_mapping_df()

        expected_field_mapping_df = pd.DataFrame(
            data={
                "Input": ["sample_input", "sample_input2"],
                "Output": ["sample_output", "sample_output2"],
                "Approved": ["yes", "no"],
            }
        )

        pd.util.testing.assert_frame_equal(
            expected_field_mapping_df, actual_field_mapping_df
        )

    def test_get_field_mapping_dict(self):
        field_mapping_df = pd.DataFrame(
            data={
                "Input": ["sample_input", "sample_input2"],
                "Output": ["sample_output", "sample_output2"],
                "Approved": ["yes", "no"],
            }
        )

        field_mapping = FieldMapping.from_dataframe(field_mapping_df)

        actual_field_mapping_dict = field_mapping.get_field_mapping_dict()

        expected_field_mapping_dict = {
            "sample_input": ("sample_output", "yes"),
            "sample_input2": ("sample_output2", "no"),
        }

        self.assertDictEqual(expected_field_mapping_dict, actual_field_mapping_dict)

    def test_if_empty_mapping_is_empty_should_be_True(self):
        field_mapping = FieldMapping.from_dict({})

        self.assertTrue(field_mapping.is_empty())

    def test_if_empty_mapping_is_not_empty_should_be_False(self):
        field_mapping = FieldMapping.from_dict({"input": ("output", "No")})

        self.assertFalse(field_mapping.is_empty())


if __name__ == "__main__":
    unittest.main()
