import logging
import tempfile

import pandas as pd
import pysftp
from airflow.hooks.base_hook import BaseHook
from etl.helpers import dates

REMOTE_FILEPATH = "./"
REMOTE_FILE_PREFIX = "mission_impact_data"


def extract_data(
    cms_info,
    drive_credentials,
    start_date,
    get_member_xcom_args,
    execution_date,
    **kwargs,
):
    member_id = kwargs["task_instance"].xcom_pull(**get_member_xcom_args)
    logging.info("Pulled a member id from `get_member` task.")
    logging.info(member_id)

    filenames = []

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(
        cms_info["host"],
        username=cms_info["username"],
        password=cms_info["password"],
        cnopts=cnopts,
    ) as sftp:
        files = sftp.listdir(REMOTE_FILEPATH)
        valid_files = [file for file in files if file.startswith(REMOTE_FILE_PREFIX)]
        for file in valid_files:
            tf = tempfile.NamedTemporaryFile(delete=False)
            sftp.get(f"{REMOTE_FILEPATH}{file}", tf.name)
            filenames.append(tf.name)

    # Read csv and extract data with the execution data range.
    for filename in filenames:
        raw_df = pd.read_csv(filename)
        data = dates.extract_date_in_range(
            raw_df, member_id, start_date, execution_date
        )

        # Re-write file.
        data.to_csv(tf.name, index=False)

    return filenames
