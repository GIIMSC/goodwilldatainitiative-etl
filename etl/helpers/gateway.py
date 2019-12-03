import io
import logging
import time
import requests


def upload_to_gateway(member_id: str, dataset_file: io.IOBase, gateway_host: str):
    """
    Uploads data from a local filesystem CSV to GII's Gateway API.
    """
    # Headers for upload
    headers = {"member_id": member_id, "user_id": "airflow-test"}

    # Metadata for upload
    data = {
        "file_format": "delimited",
        "data_profile": "mission_impact_rows",
        "delimiter": "comma",
        "header": "1",
    }

    # Open the file
    files = {"file": dataset_file}

    # POST the data
    response = requests.post(gateway_host, data=data, files=files, headers=headers)

    process_id = response.json()["process_id"]
    process_result = requests.get(
        f"https://gatewaydevdataupload.goodwill.org/api/v1.0/processes/{process_id}",
        headers=headers,
    )

    # TODO: handle invalid upload
    return process_result


def airflow_upload_to_gateway(
    transform_data_xcom_args, get_member_xcom_args, gateway_host: str, ti, **kwargs
):
    dataset_filename = ti.xcom_pull(**transform_data_xcom_args)

    member_id = ti.xcom_pull(**get_member_xcom_args)
    logging.info("Pulled a member id from `get_member` task.")
    logging.info(member_id)

    if dataset_filename is not None:
        with open(dataset_filename, "r") as f:
            upload_to_gateway(member_id, f, gateway_host)
    else:
        logging.info("No data to upload.")
