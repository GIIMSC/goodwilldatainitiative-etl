import logging
import etl.helpers.field_mapping as fm

from airflow.utils.email import send_email
from etl.helpers import common, data_processor
from etl.helpers.dataset_filter import MISSING_INTAKE_RECORD_KEY

HEADER = "This is an automated message from the GDI Pipeline.<br><br>"


def format_validation_failures(failures):
    message = "<ul>"
    for expectation_type, failed_info in sorted(failures.items()):
        failed_vals = failed_info[common.FAILED_VALUES_KEY]

        if expectation_type == common.EXPECT_COLUMNS_IN_SET_KEY:
            message += "<li>Invalid column(s) in dataset: {}</li>".format(
                ", ".join(failed_vals)
            )
        elif expectation_type == common.EXPECT_NAMED_COLS:
            message += "<li>Empty columns. Your data omits one or more header field names. Please delete (or name) the columns in the following position(s): {}</li>".format(
                ", ".join(failed_vals)
            )
        elif expectation_type == common.EXPECT_COLUMNS_MATCH_KEY:
            message += "<li>Headers are incorrect. Header row (the first row in the sheet) should exactly match [{}]</li>".format(
                ", ".join(failed_info[common.EXPECTED_ORDERED_LIST_KEY])
            )
        elif expectation_type == common.EXPECT_VALUES_IN_SET_KEY:
            message += '<li>Some values for "{}" are invalid: {}</li>'.format(
                failed_info[common.COLUMN_NAME_KEY], ", ".join(failed_vals)
            )
        elif expectation_type == common.EXPECT_VALUES_UNIQUE_KEY:
            message += '<li>No duplicates allowed for "{}". The following values had duplicates: {}</li>'.format(
                failed_info[common.COLUMN_NAME_KEY], ", ".join(failed_vals)
            )

    return message + "</ul>"


def format_unapproved_mappings(field_mappings):
    """Formats unapproved mappings as bulleted list.

    Main bullets are field names, while sub bullets are unapproved mappings.
    """
    message = "<ul>"
    for field, mapping in field_mappings.items():

        unapproved_mappings = ""
        for (
            input_val,
            (output_val, approval),
        ) in mapping.get_field_mapping_dict().items():
            if not approval == fm.common.APPROVED:
                unapproved_mappings += "<li>'{}' <b>-></b> '{}'</li>".format(
                    input_val, output_val
                )

        # Only display field name if it has unapproved mappings.
        if unapproved_mappings:
            message += "<li>{}</li><ul>{}</ul>".format(field, unapproved_mappings)

    return message + "</ul>"


def format_dropped_rows(dropped_rows):
    message = "<ul>"
    for row_info in dropped_rows:
        row = row_info[data_processor.ROW_KEY]
        message += "<li><i>(CaseNumber: '{}', MilestoneFlag: '{}')</i>".format(
            row["CaseNumber"] if "CaseNumber" in row else None,
            row["MilestoneFlag"] if "MilestoneFlag" in row else None
        )

        if row_info[MISSING_INTAKE_RECORD_KEY]:
             message += "Dropped because the rows do not have corresponding 'Intake' records in the GII MIP database.</li>"
        else:
            message += "Dropped because of {}</li>".format(", ".join(_format_dropped_row_reasons(row_info)))

    return message + "</ul>"


def _format_dropped_row_reasons(row_info):
    row = row_info[data_processor.ROW_KEY]
    return [
        f"{field} (Invalid value: {row[field]})"
        if field in row and row[field]
        else f"{field} (missing)"
        for field in row_info[data_processor.MISSING_FIELDS_KEY]
    ]


def format_dropped_vals(dropped_vals):
    message = "<ul>"
    for val_info in dropped_vals:
        message += """<li><i>(CaseNumber: '{}', Milestone: '{}')</i> Invalid value for {}: '{}'<ul><li>Reason: {}</li></ul></li>""".format(
            val_info[data_processor.CASE_NUMBER_KEY],
            val_info[data_processor.MILESTONE_FLAG_KEY],
            val_info[data_processor.FIELD_NAME_KEY],
            str(val_info[data_processor.ORIGINAL_VALUE_KEY]),
            val_info[data_processor.INVALID_REASON_KEY]
            if data_processor.INVALID_REASON_KEY in val_info
            else "",
        )
    return message + "</ul>"


def log_email(contact, email_content):
    message = (
        email_content.replace("<br>", "<br>\n")
        .replace("<ul>", "\n<ul>")
        .replace("<li>", "\n<li>")
    )
    logging.info("Sending email to address '%s' with content:\n%s", contact, message)


def airflow_email_column_mapping_validation_failure(
    contact_email: str,
    org_name: str,
    email_metadata_xcom_args: str,
    column_mapping_sheet_id: str,
    ti,
    **kwargs,
):
    subject_header = "[ACTION REQUIRED] {} Failed column mapping validation for GDI Pipeline ({})".format(
        org_name, kwargs["execution_date"].strftime("%m/%d/%y")
    )

    message = f"""Your column mappings for the GDI pipeline are invalid. Please fix this issue at:
     https://docs.google.com/spreadsheets/d/{column_mapping_sheet_id}
    <br>Note that no data will be uploaded to Gateway until this issue is fixed. Details below:<br>"""

    message += format_validation_failures(ti.xcom_pull(**email_metadata_xcom_args))

    message += "<br>Be aware that column mappings are case sensitive."

    email_content = HEADER + message
    log_email(contact_email, email_content)

    if contact_email:
        send_email(
            contact_email,
            subject_header,
            email_content,
            mime_subtype="mixed",
            mime_charset="utf8",
        )


def airflow_email_data_shape_validation_failure(
    contact_email: str,
    org_name: str,
    email_metadata_xcom_args: str,
    column_mapping_sheet_id: str,
    ti,
    **kwargs,
):
    subject_header = "[ACTION REQUIRED] {} Invalid columns in data for GDI Pipeline ({})".format(
        org_name, kwargs["execution_date"].strftime("%m/%d/%y")
    )

    message = "There are invalid columns in your data that was sent to the GDI pipeline. Details below:<br>"

    failure_map = ti.xcom_pull(**email_metadata_xcom_args)
    # Shape validation stores map of filename -> failures. Drop filename and just display failures.
    for _, failures in failure_map.items():
        message += format_validation_failures(failures)

    message += f"""<br>Note that no data will be uploaded to Gateway until this issue is fixed.
     There are three ways to fix this issue: <ul>
    <li>Remove the invalid column</li>
    <li>Change the name of the column to a valid mission impact field name.</li>
    <li>Add a mapping for this column name.Your column mappings are here:
     https://docs.google.com/spreadsheets/d/{column_mapping_sheet_id} </li></ul>"""

    message += "<br>Be aware that column mappings are case sensitive."

    email_content = HEADER + message
    log_email(contact_email, email_content)

    if contact_email:
        send_email(
            contact_email,
            subject_header,
            email_content,
            mime_subtype="mixed",
            mime_charset="utf8",
        )


def airflow_email_data_shape_required_column_failure(
    contact_email: str,
    org_name: str,
    email_metadata_xcom_args: str,
    column_mapping_sheet_id: str,
    ti,
    **kwargs,
):
    subject_header = "[ACTION REQUIRED] {} Invalid columns in data for GDI Pipeline ({})".format(
        org_name, kwargs["execution_date"].strftime("%m/%d/%y")
    )

    message = "There are invalid columns in your data that was sent to the GDI pipeline. Details below:<br>"

    failure_map = ti.xcom_pull(**email_metadata_xcom_args)
    # Shape validation stores map of filename -> failures. Drop filename and just display failures.
    for _, failures in failure_map.items():
        message += format_validation_failures(failures)

    message += f"""<br>Note that no data will be uploaded to Gateway until this issue is fixed.
     There are three ways to fix this issue: <ul>
    <li>Add the missing required column.</li>
    <li>Change the name of the column to a valid mission impact field name.</li>"
    <li>Add a mapping for this column name. Your column mappings are here:
     https://docs.google.com/spreadsheets/d/{column_mapping_sheet_id} </li></ul>"""

    email_content = HEADER + message
    log_email(contact_email, email_content)

    if contact_email:
        send_email(
            contact_email,
            subject_header,
            email_content,
            mime_subtype="mixed",
            mime_charset="utf8",
        )


def airflow_email_field_mapping_validation_failure(
    contact_email: str,
    org_name: str,
    email_metadata_xcom_args: str,
    field_mapping_sheet_id: str,
    ti,
    **kwargs,
):
    subject_header = "[ACTION REQUIRED] {} Failed field mapping validation for GDI Pipeline ({})".format(
        org_name, kwargs["execution_date"].strftime("%m/%d/%y")
    )

    message = f"""Your field mappings for the GDI pipeline are invalid. Please fix this issue at:
     https://docs.google.com/spreadsheets/d/{field_mapping_sheet_id}"
    <br>Note that no data will be uploaded to Gateway until this issue is fixed. Details below:<br><ul>"""

    failure_map = ti.xcom_pull(**email_metadata_xcom_args)

    for field_name, failures in failure_map.items():
        message += f"<li>{field_name}</li>"
        message += format_validation_failures(failures)
    message += "</ul>"

    email_content = HEADER + message
    log_email(contact_email, email_content)

    if contact_email:
        send_email(
            contact_email,
            subject_header,
            email_content,
            mime_subtype="mixed",
            mime_charset="utf8",
        )


def airflow_email_approval_mapping(
    contact_email: str,
    org_name: str,
    resolved_field_mappings_xcom_args,
    field_mapping_sheet_id: str,
    ti,
    **kwargs,
):
    """Send email indicating that field mappings need approval.

    Includes link to Google Sheet with field mappings, and list of mappings that need approval.
    """
    subject_header = "[ACTION REQUIRED] {} Approve field mappings for GDI Pipeline ({})".format(
        org_name, kwargs["execution_date"].strftime("%m/%d/%y")
    )
    message = f"""There are unapproved field mappings for your Mission Impact data.
     Please check and approve your field mappings at https://docs.google.com/spreadsheets/d/{str(field_mapping_sheet_id)}
    <br><br> If any mappings are incorrect, please fix them manually and then approve.
     Note that no data will be uploaded to Gateway until all field mappings are approved.<br><br>
    Below are the field mappings that need attention: <br>"""

    message += format_unapproved_mappings(
        ti.xcom_pull(**resolved_field_mappings_xcom_args)
    )

    message += """<br>It is also possible that these mappings were generated by invalid data.
     In that case, do not approve the new mappings. Instead, fix the data and wait for the next pipeline run."""

    email_content = HEADER + message
    log_email(contact_email, email_content)

    if contact_email:
        send_email(
            contact_email,
            subject_header,
            email_content,
            mime_subtype="mixed",
            mime_charset="utf8",
        )


def format_successful_upload(num_rows_uploaded, dropped_rows, dropped_vals):
    """Format email (HTML) for a successful upload with any dropped data.
    """
    message = f"""
    <h3>Your Mission Impact upload is complete!</h3>
    <ul class="meta-list"><li>Rows uploaded: {num_rows_uploaded}</li>
    <li>Rows dropped: {len(dropped_rows)}</li>
    <li>Individual values dropped: {len(dropped_vals)}</li></ul>
    """

    if dropped_rows or dropped_vals:
        message += "If you would like the dropped data to be uploaded on the next pipeline run, please fix your data. Details below:<br><ul>"

        if dropped_rows:
            message += f"""
            <li>The following row(s) were dropped because they had missing/invalid values for a required field(s):</li>
            {format_dropped_rows(dropped_rows)}
            """

        if dropped_vals:
            message += f"""
            <li>The following values(s) were dropped because they were missing/invalid:</li>
            {format_dropped_vals(dropped_vals)}
            """

        message += "</ul>"
    return message


def airflow_email_report(
    contact_email: str, org_name: str, email_metadata_xcom_args, ti, **kwargs
):
    """Send email indicating that data has been uploaded.

    Includes number of rows uploaded, and info about dropped rows and values.
    """
    subject_header = "[UPLOAD COMPLETE] {} Report for Mission Impact upload on {}".format(
        org_name, kwargs["execution_date"].strftime("%m/%d/%y")
    )

    dropped_data = ti.xcom_pull(**email_metadata_xcom_args)
    num_rows_uploaded = dropped_data[data_processor.NUM_ROWS_TO_UPLOAD_KEY]
    dropped_rows = dropped_data[data_processor.DROPPED_ROWS_KEY]
    dropped_vals = dropped_data[data_processor.DROPPED_VALUES_KEY]

    message = format_successful_upload(num_rows_uploaded, dropped_rows, dropped_vals)

    email_content = HEADER + message
    log_email(contact_email, email_content)

    if contact_email:
        send_email(
            contact_email,
            subject_header,
            email_content,
            mime_subtype="mixed",
            mime_charset="utf8",
        )
