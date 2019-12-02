import tempfile
import pandas as pd
import logging

from etl.helpers import drive
from etl.helpers import dates

LAST_MODIFIED_COL_NAME = "LastModifiedDate"


def extract_data(
    cms_info,
    drive_credentials,
    start_date,
    get_member_xcom_args,
    execution_date,
    **kwargs
):
    SPREADSHEET_ID = cms_info["spreadsheet_id"]

    member_id = kwargs["task_instance"].xcom_pull(**get_member_xcom_args)
    logging.info("Pulled a member id from `get_member` task.")
    logging.info(member_id)

    service = drive.get_google_sheets_service(drive_credentials)
    # Read the first 100000 rows- it'll only return ones that actually exist if
    # the sheet doesn't actually have 100000 rows in it.
    raw_df = drive.load_sheet_as_dataframe(service, SPREADSHEET_ID, "1:100000")

    if not raw_df.empty:
        data = dates.extract_date_in_range(
            raw_df, member_id, start_date, execution_date
        )
        tf = tempfile.NamedTemporaryFile(delete=False)
        data.to_csv(tf.name, index=False)
        return tf.name

    return None
