import re
import pandas as pd


import logging

def find_case_numbers(response_text: str):
    """
    This function extracts a list case numbers from a string.

    Arguments:
        - the response text from the Gateway API, which appears in the following format:
        ```
        No 'Intake' records found<br/><br/>Case: CASEID-abcde  Member: xxxx-xxxx-xxxxx<br/>Case: CASEID-fghijk  Member: xxxx-xxxx-xxxxx<br/>
        ```
    """
    pattern = r"Case:\s(.*?)\s+Member:"

    return re.findall(pattern, response_text)


def drop_rows_without_intake_records(dataframe: pd.DataFrame, response_text: str):
    case_numbers = find_case_numbers(response_text)
    dataframe = dataframe[dataframe.CaseNumber.isin(case_numbers) == False]

    return dataframe.reset_index(drop=True)


def airflow_drop_rows_without_intake_records(transform_data_as_dataframe_xcom_args, intake_error_xcom_key, ti, **kwargs):
    dataframe = ti.xcom_pull(**transform_data_as_dataframe_xcom_args)

    logging.info(dataframe)

    # drop_rows_without_intake_records()

