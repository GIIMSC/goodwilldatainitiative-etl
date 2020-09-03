import csv
import logging
import re
import tempfile

import pandas as pd


def find_case_numbers(response_text: str):
    """This function extracts a list case numbers from a string.

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


def drop_rows_from_csv_without_intake_records(datafile_name: str, response_text: str):
    case_numbers = find_case_numbers(response_text)
    tf = tempfile.NamedTemporaryFile(delete=False)

    with open(datafile_name, "rt") as mip_data_with_bad_records, open(
        tf.name, "w"
    ) as filtered_mip_data:
        reader = csv.DictReader(mip_data_with_bad_records)
        writer = csv.DictWriter(filtered_mip_data, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            if row["CaseNumber"] not in case_numbers:
                writer.writerow(row)

    return tf.name


def airflow_drop_rows_without_intake_records(
    intake_error_xcom_args, transform_data_xcom_args, ti, **kwargs
):
    """This function pulls the dataframe (from simple_pipeline) 
    and error message (from airflow_upload_to_gateway) from Airflow XComs.
    Then, this function drops 'bad' rows, saves to a TempFile, and returns the location
    of the file.
    """
    error_message = ti.xcom_pull(**intake_error_xcom_args)
    dataset_filename = ti.xcom_pull(**transform_data_xcom_args)
    filtered_dataset_filename = drop_rows_from_csv_without_intake_records(
        dataset_filename, error_message
    )

    return filtered_dataset_filename
