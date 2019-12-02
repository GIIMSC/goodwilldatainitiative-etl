import datetime
import pandas as pd
from dateutil import parser

from airflow.models import Variable

LAST_MODIFIED_COL_NAME = "LastModifiedDate"


def get_date_range(start_date, end_date):
    start_datetime = datetime.datetime.combine(
        start_date, datetime.time(0, 0, 0)
    ).isoformat()
    end_datetime = datetime.datetime.combine(
        end_date, datetime.time(23, 59, 59)
    ).isoformat()

    return start_datetime, end_datetime


def airflow_get_date_range(member_id, start_date, execution_date):
    # Get the last successful date, or if it doesn't exist,
    # the day before the pipeline's start date
    last_successful_upload_date = Variable.get(
        f"{member_id}_last_successful_upload_date",
        default_var=start_date - datetime.timedelta(days=1),
    )

    # The date will be a string if the variable exists, so parse it
    if isinstance(last_successful_upload_date, str):
        last_successful_upload_date = parser.parse(last_successful_upload_date)

    # Uploading should start from the day after the last successful upload date
    # and end on the day of execution (today)
    start_upload_date = last_successful_upload_date + datetime.timedelta(days=1)
    return get_date_range(start_upload_date, execution_date)


def extract_date_in_range(df, member_organization_id, start_date, execution_date):
    """Extracts data from dataframe that was modified between the start date and
     the execution date.
     """

    if not LAST_MODIFIED_COL_NAME in df.columns:
        return df

    start_datetime, end_datetime = airflow_get_date_range(
        member_organization_id, start_date, execution_date
    )

    # Convert last modified column to datetime.
    df[LAST_MODIFIED_COL_NAME] = pd.to_datetime(df[LAST_MODIFIED_COL_NAME])

    # Create mask to filter based on datetime range.
    mask = (df[LAST_MODIFIED_COL_NAME] >= start_datetime) & (
        df[LAST_MODIFIED_COL_NAME] <= end_datetime
    )

    data = df.loc[mask]

    # Remove last modified column, since it is not used outside of extraction.
    del data[LAST_MODIFIED_COL_NAME]

    return data
