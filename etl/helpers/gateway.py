import io
import logging
import time
import requests

# Gateway returns this message in response.text
# The ETL pipeline parses this message, filters the DataFrame, and resubmits data to Gateway.
INTAKE_ERROR = "No 'Intake' records found"

def upload_to_gateway(
    gateway_host: str, member_id: str, access_token: str, dataset_file: io.IOBase,
):
    """
    Uploads data from a local filesystem CSV to GII's Gateway API.
    """
    # Headers for upload
    headers = {"member_id": member_id, "token": access_token}

    # Metadata for upload – do we still need this?
    data = {
        "file_format": "delimited",
        "data_profile": "mission_impact_rows",
        "delimiter": "comma",
        "header": "1",
    }

    # Open the file
    files = {"file": dataset_file}

    # POST the data
    response = requests.post(gateway_host, headers=headers, data=data, files=files)

    if response.status_code is not 202 and INTAKE_ERROR not in response.text:
        logging.error(response.text)
        raise RuntimeError

        # Logic:
        # 1. Check for "No intake records errors"
        # 2. If yes, then return task_id
        # 3. If no, then raise RuntimeError
        # Think about raising: AirflowFailException - https://airflow.apache.org/docs/stable/concepts.html?highlight=branches#exceptions
        # return "drop_rows_without_intake_records"

    logging.info(response.text)
    return response.text


def airflow_upload_to_gateway(
    transform_data_xcom_args,
    get_member_xcom_args,
    get_token_xcom_args,
    gateway_host: str,
    ti,
    **kwargs,
):
    dataset_filename = ti.xcom_pull(**transform_data_xcom_args)

    access_token = ti.xcom_pull(**get_token_xcom_args)
    member_id = ti.xcom_pull(**get_member_xcom_args)
    logging.info("Pulled a member id from `get_member` task.")
    logging.info(member_id)

    if dataset_filename is not None:
        logging.info(f"Location of the file-to-upload: {dataset_filename}")
        with open(dataset_filename, "r") as file_to_upload:
            response_text = upload_to_gateway(
                gateway_host=gateway_host,
                member_id=member_id,
                access_token=access_token,
                dataset_file=file_to_upload,
            )

            if INTAKE_ERROR in response.text:
                # TODO: store the error itself somewhere...
                return "drop_rows_without_intake_records" # TODO: store in a ENV VAR
            else:
                return "send_upload_report_email"
    else:
        logging.info("No data to upload.")
