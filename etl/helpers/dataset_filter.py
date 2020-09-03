import csv
import logging
import re
import tempfile

import pandas as pd

from etl.helpers.data_processor import ROW_KEY

MISSING_INTAKE_RECORD_KEY = "is_missing_intake_record"


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


def from_csv_drop_rows_without_intake_records(datafile_name: str, response_text: str):
    case_numbers = find_case_numbers(response_text)
    tf = tempfile.NamedTemporaryFile(delete=False)
    dropped_rows = []

    with open(datafile_name, "rt") as mip_data_with_bad_records, open(
        tf.name, "w"
    ) as filtered_mip_data:
        reader = csv.DictReader(mip_data_with_bad_records)
        writer = csv.DictWriter(filtered_mip_data, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            if row["CaseNumber"] not in case_numbers:
                writer.writerow(row)
            else:
                dropped_rows.append({ROW_KEY: row, MISSING_INTAKE_RECORD_KEY: True})

    return tf.name, dropped_rows


def airflow_drop_rows_without_intake_records(
    intake_error_xcom_args,
    transform_data_xcom_args,
    email_metadata_xcom_args,
    ti,
    **kwargs
):
    """This function pulls the transformed (as processed in `simple_pipeline`) 
    and error message (as returned from `airflow_upload_to_gateway`) from Airflow XComs.
    Then, this function drops 'bad' rows, saves to a TempFile, and returns the location
    of the file.
    """
    error_message = ti.xcom_pull(**intake_error_xcom_args)
    dataset_filename = ti.xcom_pull(**transform_data_xcom_args)
    filtered_dataset_filename, dropped_rows = from_csv_drop_rows_without_intake_records(
        dataset_filename, error_message
    )

    # Update email metadata
    email_metadata = ti.xcom_pull(**email_metadata_xcom_args)
    email_metadata["num_rows_to_upload"] -= len(dropped_rows)
    email_metadata["dropped_rows"] += dropped_rows
    ti.xcom_push(key=email_metadata_xcom_args["key"], value=email_metadata)

    return filtered_dataset_filename
