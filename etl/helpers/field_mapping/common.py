import os
from typing import Dict, Tuple
import pandas as pd

INPUT_COLUMN_NAME = "Input"
INPUT_COLUMN_INDEX = 0
OUTPUT_COLUMN_NAME = "Output"
OUTPUT_COLUMN_INDEX = 1
APPROVED_COLUMN_NAME = "Approved"
APPROVED_COLUMN_INDEX = 2
COLUMN_NAMES = [INPUT_COLUMN_NAME, OUTPUT_COLUMN_NAME, APPROVED_COLUMN_NAME]

APPROVED = "Yes"
NOT_APPROVED = "No"
VALID_APPROVED_VALUES = [APPROVED, NOT_APPROVED]


def get_field_mapping_filename(field_name: str, config_location: str) -> str:
    """Returns the expected filename for a field mapping."""
    return os.path.join(config_location, field_name + ".csv")


class FieldMapping:
    FieldMappingDF = pd.DataFrame
    FieldMappingDict = Dict[str, Tuple[str, str]]

    def __init__(
        self,
        field_mapping_dict: FieldMappingDict = None,
        field_mapping_df: FieldMappingDF = None,
    ):
        self._field_mapping_dict = field_mapping_dict
        self._field_mapping_df = field_mapping_df

    @classmethod
    def from_dataframe(cls, field_mapping_df: FieldMappingDF):
        return cls(field_mapping_df=field_mapping_df)

    @classmethod
    def from_dict(cls, field_mapping_dict: FieldMappingDict):
        return cls(field_mapping_dict=field_mapping_dict)

    @staticmethod
    def _convert_field_mapping_df_to_dict(
        field_mapping_df: FieldMappingDF,
    ) -> FieldMappingDict:
        if field_mapping_df is None:
            return {}
        df_as_dict = field_mapping_df.set_index(INPUT_COLUMN_NAME).to_dict()

        output_dict = df_as_dict[OUTPUT_COLUMN_NAME]
        approved_dict = df_as_dict[APPROVED_COLUMN_NAME]

        final_dict = {}
        for input, output in output_dict.items():
            final_dict[input] = (output, approved_dict[input])

        return final_dict

    @staticmethod
    def _convert_field_mapping_dict_to_df(
        field_mapping_dict: FieldMappingDict,
    ) -> FieldMappingDF:
        if field_mapping_dict is None:
            return None

        inputs = []
        outputs = []
        approved_vals = []
        for input, (output, approved) in field_mapping_dict.items():
            inputs.append(input)
            outputs.append(output)
            approved_vals.append(approved)

        reshaped_dict = {
            INPUT_COLUMN_NAME: inputs,
            OUTPUT_COLUMN_NAME: outputs,
            APPROVED_COLUMN_NAME: approved_vals,
        }
        return pd.DataFrame.from_dict(reshaped_dict)

    def get_field_mapping_df(self) -> FieldMappingDF:
        if self._field_mapping_df is None:
            self._field_mapping_df = FieldMapping._convert_field_mapping_dict_to_df(
                self._field_mapping_dict
            )
        return self._field_mapping_df

    def get_field_mapping_dict(self) -> FieldMappingDict:
        if not self._field_mapping_dict:
            self._field_mapping_dict = FieldMapping._convert_field_mapping_df_to_dict(
                self._field_mapping_df
            )
        return self._field_mapping_dict

    def is_empty(self):
        return not self.get_field_mapping_dict()


FieldMappings = Dict[str, FieldMapping]
