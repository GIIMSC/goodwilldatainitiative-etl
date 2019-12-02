# IDC Mission Impact Pipeline

This repository contains the ETL pipeline code for the [Airflow tool](https://github.com/GIIMSC/GoodwillDataInitiative) and [Flask app](https://github.com/GIIMSC/goodwilldatainitiative-webuploader) that process Mission Impact data.

## Code organization

* helpers: Pipeline business logic
* pipeline: Contains a standalone version of the pipeline steps that can be run outside of airflow
* schemasschemas: All schemas used for this project, in [Table Schema](https://frictionlessdata.io/specs/table-schema/) format

## Tests

Run tests with: `python -m pytest`