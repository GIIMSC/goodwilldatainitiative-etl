import logging
import tempfile
from typing import Dict, List

import jinja2
import pandas as pd
from airflow.contrib.hooks.salesforce_hook import SalesforceHook
from etl.helpers import dates, drive


def parse_sf_record(nested_dict: Dict) -> Dict:
    """Recursively parse the nested dictionaries returned by Salesforce Simple API
    library.
    :param nested_dict: Nested dictionary object
    :return: Flattened dictionary representing record
    """

    clean_dict: Dict = {}

    for k, v in nested_dict.items():
        if k == "attributes" or v is None:
            continue
        elif isinstance(v, dict):
            clean_child_dict = parse_sf_record(v)

            for child_key, child_clean_value in clean_child_dict.items():
                clean_key = f"{k}.{child_key}"
                clean_dict[clean_key] = child_clean_value
        else:
            clean_dict[k] = v

    return clean_dict


def _get_sf_records(sf_hook: SalesforceHook, query: str) -> pd.DataFrame:
    query_result = sf_hook.make_query(query)
    records = [parse_sf_record(record) for record in query_result["records"]]
    df = pd.DataFrame.from_records(records)
    return df


def airflow_extract_data(
    cms_info,
    drive_credentials,
    start_date,
    get_member_xcom_args,
    execution_date,
    **kwargs,
) -> List[str]:
    """Extracts data from Salesforce that was modified between the start date
    and the execution date.
    """
    member_id = kwargs["task_instance"].xcom_pull(**get_member_xcom_args)
    logging.info("Pulled a member id from `get_member` task.")
    logging.info(member_id)

    CONN_ID = cms_info["connection_id"]
    QUERIES = cms_info["queries"]

    sf_hook: SalesforceHook = SalesforceHook(conn_id=CONN_ID)
    docs_service = drive.get_google_docs_service(drive_credentials)

    start_datetime, end_datetime = dates.airflow_get_date_range(
        member_id, start_date, execution_date
    )

    filenames: List[str] = []

    for document_id in QUERIES:
        # Load the query and replace the dates
        query: str = drive.load_doc_as_query(docs_service, document_id)
        template = jinja2.Template(query)
        query_with_dates: str = template.render(
            start_datetime=f"{start_datetime}Z", end_datetime=f"{end_datetime}Z"
        )

        # Get the data and write it to a .csv
        records_dataframe: pd.DataFrame = _get_sf_records(sf_hook, query_with_dates)
        if not records_dataframe.empty:
            tf = tempfile.NamedTemporaryFile(delete=False)
            records_dataframe.to_csv(tf.name, index=False)
            filenames.append(tf.name)

    return filenames
