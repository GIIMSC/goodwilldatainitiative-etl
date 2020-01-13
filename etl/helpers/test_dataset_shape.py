# import unittest
# import typing
# import numpy as np
# import pandas as pd
# import tableschema

# from etl.helpers import common, dataset_shape

# MEMBER_ORGANIZATION_ID = "sample_id"

# TEST_SCHEMA: tableschema.Schema = tableschema.Schema(
#     {
#         "fields": [
#             {"name": "field1"},
#             {"name": "field2"},
#             {"name": "field3", "allows_multiple": True},
#         ]
#     }
# )

# TEST_COLUMN_MAPPING: typing.Dict[str, str] = {
#     "internal_column_name1": "field1",
#     "internal_column_name2": "field2",
#     "internal_column_name3": "field3",
# }

# TEST_SCHEMA_COL: tableschema.Schema = tableschema.Schema(
#     {
#         "fields": [
#             {"name": "field1"},
#             {"name": "field2", "milestones": [0]},
#             {"name": "field3", "milestones": [0, 1]},
#             {
#                 "name": "field4",
#                 "milestones": [2],
#                 "custom_milestone_field_names": {"2": "actual_field4"},
#             },
#         ],
#         "column_based_milestone_names": ["Intake", "Exit", "NinetyDays"],
#     }
# )


# class DatasetShapeValidatorTest(unittest.TestCase):
#     def test_validate_dataset_shape_row_format_all_columns_mapped(self):
#         dataset_shape_validator = dataset_shape.DatasetShapeValidator(
#             TEST_SCHEMA, TEST_COLUMN_MAPPING, row_format=True
#         )
#         dataset = pd.DataFrame(
#             data={
#                 "internal_column_name1": [1, 2],
#                 "internal_column_name2": [3, 4],
#                 "internal_column_name3": [3, 4],
#             }
#         )
#         validation_failures = dataset_shape_validator.validate_multiple_dataset_shape(
#             {"dataset1": dataset}
#         )
#         self.assertFalse(validation_failures)

#     def test_validate_dataset_shape_row_format_some_columns_in_schema(self):
#         dataset_shape_validator = dataset_shape.DatasetShapeValidator(
#             TEST_SCHEMA, TEST_COLUMN_MAPPING, row_format=True
#         )
#         dataset = pd.DataFrame(
#             data={
#                 "internal_column_name1": [1, 2],
#                 "internal_column_name2": [3, 4],
#                 "field3": [3, 4],
#             }
#         )

#         validation_failures = dataset_shape_validator.validate_multiple_dataset_shape(
#             {"dataset1": dataset}
#         )
#         self.assertFalse(validation_failures)

#     def test_validate_dataset_shape_row_format_some_columns_not_mapped_or_in_schema(
#         self,
#     ):
#         dataset_shape_validator = dataset_shape.DatasetShapeValidator(
#             TEST_SCHEMA, TEST_COLUMN_MAPPING, row_format=True
#         )
#         dataset = pd.DataFrame(data={"random_column_name": [1, 2]})

#         validation_failures = dataset_shape_validator.validate_multiple_dataset_shape(
#             {"dataset1": dataset}
#         )
#         self.assertTrue(validation_failures)
#         self.assertEqual(
#             {
#                 "dataset1": {
#                     common.EXPECT_COLUMNS_IN_SET_KEY: {
#                         common.FAILED_VALUES_KEY: ["random_column_name"]
#                     }
#                 }
#             },
#             validation_failures,
#         )

#     def test_validate_dataset_shape_col_format(self):
#         dataset_shape_validator = dataset_shape.DatasetShapeValidator(
#             TEST_SCHEMA_COL, {}, row_format=False
#         )
#         dataset = pd.DataFrame(
#             data={
#                 "field1": ["field1_1", "field1_2"],
#                 "Intakefield2": ["field2_1", "field2_2"],
#                 "Intakefield3": ["field3_1", "field3_2"],
#                 "Exitfield3": ["field3_3", "field3_4"],
#                 "actual_field4": ["field4_1", "field4_1"],
#             }
#         )

#         validation_failures = dataset_shape_validator.validate_multiple_dataset_shape(
#             {"dataset1": dataset}
#         )
#         self.assertFalse(validation_failures)

#     def test_validate_dataset_shape_col_format_some_columns_not_in_schema(self):
#         dataset_shape_validator = dataset_shape.DatasetShapeValidator(
#             TEST_SCHEMA_COL, {}, row_format=False
#         )
#         dataset = pd.DataFrame(
#             data={
#                 "field1": [1, 2],
#                 "Intakefield2": [3, 4],
#                 "Intakefield3": [3, 4],
#                 "Exitfield3": [3, 4],
#                 "WrongField4": [3, 4],
#                 "WrongField5": [3, 4],
#             }
#         )

#         validation_failures = dataset_shape_validator.validate_multiple_dataset_shape(
#             {"dataset1": dataset}
#         )
#         self.assertTrue(validation_failures)
#         self.assertEqual(
#             {
#                 "dataset1": {
#                     common.EXPECT_COLUMNS_IN_SET_KEY: {
#                         common.FAILED_VALUES_KEY: ["WrongField4", "WrongField5"]
#                     }
#                 }
#             },
#             validation_failures,
#         )


# class DatasetShapeTest(unittest.TestCase):
#     def test_transform_dataset_shape_datset_is_empty(self):
#         dataset_shape_transformer = dataset_shape.DatasetShapeTransformer(
#             MEMBER_ORGANIZATION_ID, TEST_SCHEMA, TEST_COLUMN_MAPPING, row_format=True
#         )

#         empty_dataset = pd.DataFrame(columns=[])

#         actual_shaped_dataset = dataset_shape_transformer.transform_dataset_shape(
#             empty_dataset
#         )

#         expected_shaped_dataset = pd.DataFrame(columns=[])
#         pd.util.testing.assert_frame_equal(
#             expected_shaped_dataset, actual_shaped_dataset
#         )

#     def test_transform_dataset_shape_row_format_all_columns_present(self):
#         dataset_shape_transformer = dataset_shape.DatasetShapeTransformer(
#             MEMBER_ORGANIZATION_ID, TEST_SCHEMA, TEST_COLUMN_MAPPING, row_format=True
#         )
#         dataset = pd.DataFrame(
#             data={
#                 "internal_column_name1": [1, 2],
#                 "internal_column_name2": [3, 4],
#                 "internal_column_name3": [3, 4],
#             }
#         )

#         actual_shaped_dataset = dataset_shape_transformer.transform_dataset_shape(
#             dataset
#         )

#         expected_column_names = [
#             "field1",
#             "field2",
#             "field3",
#             "MemberOrganization",
#             "ForceOverWrite",
#         ]
#         self.assertTrue(
#             all(
#                 [
#                     a == b
#                     for a, b in zip(
#                         actual_shaped_dataset.columns.values, expected_column_names
#                     )
#                 ]
#             )
#         )

#     def test_transform_dataset_shape_row_format_some_columns_present(self):
#         dataset_shape_transformer = dataset_shape.DatasetShapeTransformer(
#             MEMBER_ORGANIZATION_ID, TEST_SCHEMA, TEST_COLUMN_MAPPING, row_format=True
#         )
#         dataset = pd.DataFrame(
#             data={"internal_column_name1": [1, 2], "internal_column_name2": [3, 4]}
#         )

#         actual_shaped_dataset = dataset_shape_transformer.transform_dataset_shape(
#             dataset
#         )

#         expected_column_names = [
#             "field1",
#             "field2",
#             "MemberOrganization",
#             "ForceOverWrite",
#         ]
#         self.assertTrue(
#             all(
#                 [
#                     a == b
#                     for a, b in zip(
#                         actual_shaped_dataset.columns.values, expected_column_names
#                     )
#                 ]
#             )
#         )

#     def test_transform_dataset_shape_row_format_no_columns_present(self):
#         dataset_shape_transformer = dataset_shape.DatasetShapeTransformer(
#             MEMBER_ORGANIZATION_ID, TEST_SCHEMA, TEST_COLUMN_MAPPING, row_format=True
#         )
#         dataset = pd.DataFrame(data={})

#         actual_shaped_dataset = dataset_shape_transformer.transform_dataset_shape(
#             dataset
#         )

#         expected_column_names = ["MemberOrganization", "ForceOverWrite"]
#         self.assertTrue(
#             all(
#                 [
#                     a == b
#                     for a, b in zip(
#                         actual_shaped_dataset.columns.values, expected_column_names
#                     )
#                 ]
#             )
#         )

#     def test_transform_dataset_shape_col_format(self):
#         dataset_shape_transformer = dataset_shape.DatasetShapeTransformer(
#             MEMBER_ORGANIZATION_ID, TEST_SCHEMA_COL, {}, row_format=False
#         )
#         dataset = pd.DataFrame(
#             data={
#                 "field1": ["field1_1", "field1_2"],
#                 "Intakefield2": ["field2_1", "field2_2"],
#                 "Intakefield3": ["field3_1", "field3_2"],
#                 "Exitfield3": ["field3_3", "field3_4"],
#                 "actual_field4": ["field4_1", "field4_1"],
#             }
#         )

#         actual_shaped_dataset = dataset_shape_transformer.transform_dataset_shape(
#             dataset
#         )

#         expected_shaped_dataset = pd.DataFrame(
#             data={
#                 "MilestoneFlag": [
#                     "Intake",
#                     "Intake",
#                     "Exit",
#                     "Exit",
#                     "NinetyDays",
#                     "NinetyDays",
#                 ],
#                 "field1": [
#                     "field1_1",
#                     "field1_2",
#                     "field1_1",
#                     "field1_2",
#                     "field1_1",
#                     "field1_2",
#                 ],
#                 "field2": ["field2_1", "field2_2", "", "", "", ""],
#                 "field3": ["field3_1", "field3_2", "field3_3", "field3_4", "", ""],
#                 "field4": ["", "", "", "", "field4_1", "field4_1"],
#                 "MemberOrganization": [
#                     "sample_id",
#                     "sample_id",
#                     "sample_id",
#                     "sample_id",
#                     "sample_id",
#                     "sample_id",
#                 ],
#                 "ForceOverWrite": ["1", "1", "1", "1", "1", "1"],
#             }
#         )
#         pd.util.testing.assert_frame_equal(
#             expected_shaped_dataset, actual_shaped_dataset
#         )

#     def test_transform_dataset_shape_multiple_values(self):
#         dataset_shape_transformer = dataset_shape.DatasetShapeTransformer(
#             MEMBER_ORGANIZATION_ID,
#             TEST_SCHEMA,
#             TEST_COLUMN_MAPPING,
#             row_format=True,
#             multiple_val_delimiter=",",
#         )
#         dataset = pd.DataFrame(
#             data={
#                 "internal_column_name1": ["1", "2"],
#                 "internal_column_name2": ["3", "4"],
#                 "internal_column_name3": ["3,5", "4"],
#             }
#         )

#         actual_shaped_dataset = dataset_shape_transformer.transform_dataset_shape(
#             dataset
#         )

#         expected_shaped_dataset = pd.DataFrame(
#             data={
#                 "field1": ["1", "2"],
#                 "field2": ["3", "4"],
#                 "field3": [["3", "5"], ["4"]],
#                 "MemberOrganization": ["sample_id", "sample_id"],
#                 "ForceOverWrite": ["1", "1"],
#             }
#         )

#         pd.util.testing.assert_frame_equal(
#             expected_shaped_dataset, actual_shaped_dataset
#         )

#     def test_transform_dataset_shape_multiple_values_blank_values(self):
#         dataset_shape_transformer = dataset_shape.DatasetShapeTransformer(
#             MEMBER_ORGANIZATION_ID,
#             TEST_SCHEMA,
#             TEST_COLUMN_MAPPING,
#             row_format=True,
#             multiple_val_delimiter=",",
#         )
#         dataset = pd.DataFrame(
#             data={
#                 "internal_column_name1": ["1", "2"],
#                 "internal_column_name2": ["3", "4"],
#                 "internal_column_name3": ["", "4"],
#             }
#         )

#         actual_shaped_dataset = dataset_shape_transformer.transform_dataset_shape(
#             dataset
#         )

#         expected_shaped_dataset = pd.DataFrame(
#             data={
#                 "field1": ["1", "2"],
#                 "field2": ["3", "4"],
#                 "field3": [[""], ["4"]],
#                 "MemberOrganization": ["sample_id", "sample_id"],
#                 "ForceOverWrite": ["1", "1"],
#             }
#         )

#         pd.util.testing.assert_frame_equal(
#             expected_shaped_dataset, actual_shaped_dataset
#         )

#     def test_transform_dataset_shape_strips_whitespace(self):
#         dataset_shape_transformer = dataset_shape.DatasetShapeTransformer(
#             MEMBER_ORGANIZATION_ID,
#             TEST_SCHEMA,
#             TEST_COLUMN_MAPPING,
#             row_format=True,
#             multiple_val_delimiter=",",
#         )
#         dataset = pd.DataFrame(data={"field1": ["  1", "2   ", "   3   "]})

#         actual_shaped_dataset = dataset_shape_transformer.transform_dataset_shape(
#             dataset
#         )

#         expected_shaped_dataset = pd.DataFrame(
#             data={
#                 "field1": ["1", "2", "3"],
#                 "MemberOrganization": ["sample_id", "sample_id", "sample_id"],
#                 "ForceOverWrite": ["1", "1", "1"],
#             }
#         )

#         pd.util.testing.assert_frame_equal(
#             expected_shaped_dataset, actual_shaped_dataset
#         )


# class GatewayDatasetShapeTransformerTest(unittest.TestCase):
#     def test_transform_dataset_shape(self):
#         dataset = pd.DataFrame(
#             data={"field1": [1, 2], "field2": [3, 4], "field3": [[3, 4], [2, 4]]}
#         )

#         actual_shaped_dataset = dataset_shape.GatewayDatasetShapeTransformer(
#             TEST_SCHEMA
#         ).transform_dataset_shape(dataset)

#         expected_shaped_dataset = dataset = pd.DataFrame(
#             data={"field1": [1, 2], "field2": [3, 4], "field3": ["3,4", "2,4"]}
#         )

#         pd.util.testing.assert_frame_equal(
#             expected_shaped_dataset, actual_shaped_dataset
#         )


# if __name__ == "__main__":
#     unittest.main()
