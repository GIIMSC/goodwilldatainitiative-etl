import pandas as pd
from typing import List, Dict
from tableschema import Schema
import logging
from functools import partial
import tempfile

from etl.helpers import drive, email, column_mapping, table_schema
from etl.helpers.data_processor import (
    DataProcessor,
    NUM_ROWS_TO_UPLOAD_KEY,
    DROPPED_ROWS_KEY,
    DROPPED_VALUES_KEY,
)
from etl.helpers.column_mapping import ColumnMappingLoader
from etl.helpers.column_mapping import ColumnMappingValidator
from etl.helpers.field_mapping.common import FieldMappings
from etl.helpers.field_mapping.validator import (
    FieldMappingValidator,
    FieldMappingApprovalValidator,
)
from etl.helpers.field_mapping.generator import FieldMappingGenerator
from etl.helpers.field_mapping.resolver import FieldMappingResolver
from etl.helpers.field_mapping.loader import FieldMappingLoader
from etl.helpers.field_mapping.writer import FieldMappingWriter
from etl.helpers.dataset_shape import (
    DatasetShapeValidator,
    DatasetShapeTransformer,
    GatewayDatasetShapeTransformer,
)

# Keys for return_vals map in simple_pipeline.
DATASET_RETURN_KEY = "dataset"
FIELD_MAPPINGS_RETURN_KEY = "field_mappings"
FAILURE_EMAIL_TASK_ID_KEY = "failure_email_task_id"
EMAIL_METADATA_KEY = "email_metadata"

# Task IDs used for branching.
SEND_COLUMN_MAPPING_INVALID_EMAIL_TASK_ID = "send_column_mapping_invalid_email"
SEND_DATA_SHAPE_INVALID_EMAIL_TASK_ID = "send_data_shape_invalid_email"
SEND_FIELD_MAPPING_INVALID_EMAIL_TASK_ID = "send_field_mapping_invalid_email"
SEND_FIELD_MAPPING_APPROVAL_EMAIL_TASK_ID = "send_field_mapping_approval_email"
UPLOAD_DATA_TASK_ID = "upload_data"
WRITE_FIELD_MAPPINGS_TASK_ID = "write_field_mappings"

# Dummy operator task ids
FIELD_MAPPING_APPROVAL_DUMMY_TASK_ID = "field_mapping_approval_dummy_task"
TRANSFORMATION_SUCCESSFUL_DUMMY_TASK_ID = "transformation_successful_dummy_task"


def simple_pipeline(
    member_id: str,
    row_format: bool,
    multiple_val_delimiter: str,
    data: Dict[str, pd.DataFrame],
    schema: Schema,
    column_mapping: pd.DataFrame,
    source_field_mappings: FieldMappings,
):
    """Simple pipeline to transform Mission Impact data to prepare it for upload
     to the Gateway system.

    Note that log changes are not noops; logs are captured and sent directly
    to users of the web endpoint, as well as saved by the Airflow server, so readability
    of all logs of level INFO and above is critical.

    Parameters
    ----------
    member_id : str
        The organization's Member ID.
    row_format : bool
        Whether the data is organized using the Mission Impact Row Format (if
        this is false, it implies that the data is organiaed using the Mission
        Impact Column Format).
    multiple_val_delimiter : str
        The separator for multiple values in the dataset.
    data : Dict[str, pd.DataFrame]
        Dictionary of {dataset name -> dataset}.
    schema: Schema
        Mission Impact Table Schema
    column_mapping : pd.DataFrame
        Column Mapping.
    source_field_mappings : FieldMappings
        Field Mappings.

    Returns
    -------
    type
        Returns the transformed dataset and any resolved field mappings.

    """

    return_val = {}

    # Validate Table Schema
    table_schema.validate_schema(schema)

    # Validate Column Mappings
    validation_failures = ColumnMappingValidator(schema, row_format).validate(
        column_mapping
    )

    if validation_failures:
        logging.error(
            "The pipeline could not finish, because some of your column mappings are not valid. Please review the following names in your Column Mappings Google sheet:<br>"
        )
        logging.error(email.format_validation_failures(validation_failures))
        return_val[EMAIL_METADATA_KEY] = validation_failures
        return_val[
            FAILURE_EMAIL_TASK_ID_KEY
        ] = SEND_COLUMN_MAPPING_INVALID_EMAIL_TASK_ID

        return return_val

    column_mapping = ColumnMappingLoader.convert_column_mapping_dataframe_to_dict(
        column_mapping
    )

    # Validate Field Mappings
    validation_failures: Dict[str, Dict] = FieldMappingValidator(
        schema
    ).validate_multiple(source_field_mappings)

    if validation_failures:
        logging.error("Field mappings are not valid!")
        for _, validation_failure in validation_failures.items():
            logging.error(email.format_validation_failures(validation_failure))
        return_val[EMAIL_METADATA_KEY] = validation_failures
        return_val[FAILURE_EMAIL_TASK_ID_KEY] = SEND_FIELD_MAPPING_INVALID_EMAIL_TASK_ID
        return return_val

    # Validate Data Shape
    validation_failures = DatasetShapeValidator(
        schema, column_mapping, row_format
    ).validate_multiple_dataset_shape(data)

    if validation_failures:
        logging.error("Dataset shape is not valid!")
        for _, validation_failure in validation_failures.items():
            logging.error(email.format_validation_failures(validation_failure))
        return_val[EMAIL_METADATA_KEY] = validation_failures
        return_val[FAILURE_EMAIL_TASK_ID_KEY] = SEND_DATA_SHAPE_INVALID_EMAIL_TASK_ID
        return return_val

    # Shape Data
    shape_transformer: DatasetShapeTransformer = DatasetShapeTransformer(
        member_id, schema, column_mapping, row_format, multiple_val_delimiter
    )

    # TODO: Move concatentation of multiple datasets into DatasetShapeTransformer
    # Combine all of the datasets into one
    combined_shaped_dataset: pd.DataFrame = pd.concat(
        [shape_transformer.transform_dataset_shape(df) for df in data.values()],
        ignore_index=True,
        sort=True,
    )

    combined_shaped_dataset = combined_shaped_dataset.fillna("")

    # Generate Field mappings
    generated_field_mappings: FieldMappings = FieldMappingGenerator(
        schema
    ).generate_mappings_from_dataset(combined_shaped_dataset)

    # Resolve Field Mappings
    resolved_field_mappings: FieldMappings = FieldMappingResolver.resolve_mappings(
        generated_field_mappings,
        source_field_mappings,
        overwrite=False,
        remove_unapproved_source_mappings=True,
    )

    return_val[FIELD_MAPPINGS_RETURN_KEY] = resolved_field_mappings

    # Validate Field Mapping Approvals
    validation_failures: Dict[
        str, Dict
    ] = FieldMappingApprovalValidator().validate_multiple(resolved_field_mappings)

    if validation_failures:
        logging.error(
            'The pipeline could not finish, because some of your field mappings do not have approved values. Most likely, your data has new responses, which require new mappings. Go to your Field Mappings Google sheet, and approve the new mappings by toggling "No" to "Yes" on the following fields:<br>'
        )
        logging.error(email.format_unapproved_mappings(resolved_field_mappings))
        # No need to store email metadata, since resolved field mappings are used to generate
        # field mapping approval needed email.
        return_val[
            FAILURE_EMAIL_TASK_ID_KEY
        ] = SEND_FIELD_MAPPING_APPROVAL_EMAIL_TASK_ID
        return return_val

    # Process Data
    transformed_dataset, invalid_values, dropped_rows = DataProcessor(
        resolved_field_mappings, schema
    ).process(combined_shaped_dataset)

    final_shaped_dataset = GatewayDatasetShapeTransformer(
        schema
    ).transform_dataset_shape(transformed_dataset)

    # Store number of rows in processed data, plus dropped data info.
    logging.warning(
        "<br>"
        + email.format_successful_upload(
            final_shaped_dataset.shape[0], dropped_rows, invalid_values
        )
    )
    return_val[EMAIL_METADATA_KEY] = {
        NUM_ROWS_TO_UPLOAD_KEY: final_shaped_dataset.shape[0],
        DROPPED_ROWS_KEY: dropped_rows,
        DROPPED_VALUES_KEY: invalid_values,
    }

    return_val[DATASET_RETURN_KEY] = final_shaped_dataset

    return return_val


def from_local(
    member_id: str,
    row_format: bool,
    multiple_val_delimiter: str,
    schema_filename: str,
    column_mapping_filename: str,
    field_mappings_filename: str,
    extracted_data_filenames: List[str],
):
    """Runs the simple pipeline using column and field mappings stored in the
    local filesystem.

    Parameters
    ----------
    schema_filename : str:
        Local filename for the Mission Impact table schema.
    column_mapping_filename : str
        Local filename for the dataset's column mapping.
    field_mappings_filename : str
        Local filename for the datset's field mappings.

    Returns
    -------
    type
        Returns the transformed dataset and any resolved field mappings.

    """

    # Check if there are files
    if not extracted_data_filenames:
        logging.info("No data found. Ending task.")
        return

    if isinstance(extracted_data_filenames, str):
        extracted_data_filenames = [extracted_data_filenames]

    all_data: Dict[str, pd.DataFrame] = {}
    for filename in extracted_data_filenames:
        all_data[filename] = pd.read_csv(filename)

    schema: Schema = table_schema.get_schema(schema_filename)

    column_mapping = ColumnMappingLoader().load_column_mappings_local(
        column_mapping_filename
    )

    source_field_mappings = FieldMappingLoader(schema).load_field_mappings_local(
        field_mappings_filename
    )

    return simple_pipeline(
        member_id,
        row_format,
        multiple_val_delimiter,
        all_data,
        schema,
        column_mapping,
        source_field_mappings,
    )


def airflow_from_drive(
    # member_id: str,
    row_format: bool,
    multiple_val_delimiter: str,
    load_schema_xcom_args,
    column_mapping_xcom_args,
    load_field_mappings_xcom_args,
    extract_data_xcom_args,
    get_member_xcom_args,
    email_metadata_xcom_key: str,
    resolved_field_mappings_xcom_key: str,
    transformed_data_xcom_key: str,
    ti,
    **kwargs
):
    """Runs the simple pipeline for processing data in airflow and stores any
    resolved field mappings and transformed datasets in th appropriate xcoms.

    Parameters
    ----------
    load_schema_xcom_args : type
        XCOM identifier for the loaded schema.
    column_mapping_xcom_args : type
        XCOM identifier for the loaded column_mapping.
    load_field_mappings_xcom_args : type
        XCOM identifier for the loaded field mappings.
    extract_data_xcom_args : type
        XCOM identifier for the extracted data files.
    failure_email_task_id_xcom_args : type
        XCOM identifier for the email to be sent if pipeline validation fails.
    get_member_xcom_args : type
        XCOM identifier for the member id
    email_metadata_xcom_key: str
        XCOM key to store metadata to create email.
    resolved_field_mappings_xcom_key : str
        XCOM key to store resolved field mappings.
    transformed_data_xcom_key : str
        XCOM key to store transformed data.
    ti : type
        Airflow task instance.
    **kwargs : type
        Additional Airflow context parameters.

    """
    schema: Schema = ti.xcom_pull(**load_schema_xcom_args)
    column_mapping: pd.DataFrame = ti.xcom_pull(**column_mapping_xcom_args)
    source_field_mappings: pd.DataFrame = ti.xcom_pull(**load_field_mappings_xcom_args)
    extracted_data_filenames = ti.xcom_pull(**extract_data_xcom_args)

    member_id = ti.xcom_pull(**get_member_xcom_args)
    logging.info("Pulled a member id from `get_member` task.")
    logging.info(member_id)

    # Check if there are files
    if not extracted_data_filenames:
        logging.info("No data file(s) found. Ending task.")
        # TODO(joeljacobs): Send email even if no data picked up.
        return []

    if isinstance(extracted_data_filenames, str):
        extracted_data_filenames = [extracted_data_filenames]

    all_data: Dict[str, pd.DataFrame] = {}
    for filename in extracted_data_filenames:
        all_data[filename] = pd.read_csv(filename)

    if all([df.empty for df in all_data.values()]):
        logging.info("Data file(s) are empty. Ending task.")
        # TODO(joeljacobs): Send email even if no data picked up.
        return []

    return_vals = simple_pipeline(
        member_id,
        row_format,
        multiple_val_delimiter,
        all_data,
        schema,
        column_mapping,
        source_field_mappings,
    )

    # Push email metadata
    email_metadata = (
        return_vals[EMAIL_METADATA_KEY] if EMAIL_METADATA_KEY in return_vals else None
    )
    ti.xcom_push(key=email_metadata_xcom_key, value=email_metadata)

    # Push resolved field mappings
    resolved_mappings = (
        return_vals[FIELD_MAPPINGS_RETURN_KEY]
        if FIELD_MAPPINGS_RETURN_KEY in return_vals
        else None
    )
    ti.xcom_push(key=resolved_field_mappings_xcom_key, value=resolved_mappings)

    # Push transformed dataset
    transformed_dataset = (
        return_vals[DATASET_RETURN_KEY] if DATASET_RETURN_KEY in return_vals else None
    )
    if transformed_dataset is None:
        ti.xcom_push(key=transformed_data_xcom_key, value=None)
    else:
        tf = tempfile.NamedTemporaryFile(delete=False)
        transformed_dataset.to_csv(tf.name)
        ti.xcom_push(key=transformed_data_xcom_key, value=tf.name)

    # # If field mappings are resolved, add task to write them.
    # next_task_list = (
    #     [WRITE_FIELD_MAPPINGS_TASK_ID]
    #     if FIELD_MAPPINGS_RETURN_KEY in return_vals
    #     else []
    # )

    # # If simple_pipeline returns a failure email task id, then some validation failed,
    # # and a failure email should be sent. Otherwise, there were no validation errors,
    # # and the data should be uploaded to Gateway.
    # if FAILURE_EMAIL_TASK_ID_KEY in return_vals:
    #     next_task_list.append(return_vals[FAILURE_EMAIL_TASK_ID_KEY])
    # else:
    #     next_task_list.append(UPLOAD_DATA_TASK_ID)

    # logging.info(next_task_list)

    # return next_task_list

    # TODO(joeljacobs): Replace this block of code with the commented block above
    # once airflow version 1.10.3, which allows multiple branching, is available on Docker.
    if (
        FAILURE_EMAIL_TASK_ID_KEY in return_vals
        and return_vals[FAILURE_EMAIL_TASK_ID_KEY]
        == SEND_FIELD_MAPPING_APPROVAL_EMAIL_TASK_ID
    ):
        return FIELD_MAPPING_APPROVAL_DUMMY_TASK_ID
    elif FAILURE_EMAIL_TASK_ID_KEY in return_vals:
        return return_vals[FAILURE_EMAIL_TASK_ID_KEY]
    else:
        ### Log the length of transformed_dataset here!
        # row_count, col_count = transformed_dataset.shape
        row_count = len(transformed_dataset)

        logging.info(
            "The pipeline transformed {} valid rows to be uploaded to Gateway.".format(
                row_count
            )
        )
        return TRANSFORMATION_SUCCESSFUL_DUMMY_TASK_ID
