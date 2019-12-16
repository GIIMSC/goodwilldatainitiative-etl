# GoodwillDataInitiative ETL

`goodwilldatainitiative-etl` contains a collection of scripts that extract Mission Impact (MI) data from local Goodwills, transform the data into the Goodwill International (GII) MI schema, and load the data into GII Gateway. Two tools use these scripts: an [Airflow tool](https://github.com/GIIMSC/goodwilldatainitiative-airflow), which runs the automated pull/push solution, and a [Flask app](https://github.com/GIIMSC/goodwilldatainitiative-idcupload), which provides a user interface for manual uploads.

## Code organization

The `etl` directory has three subdirectories.

**`helpers`**. The ETL business logic. 

**`pipeline`**. A standalone version of the transformation process. `simple_pipeline.py` runs the "T" of the ETL pipeline: it accepts local Goodwill credentials and MI data, and it returns data in the GII MI schema. This script, in particular, serves the [Flask app](https://github.com/GIIMSC/goodwilldatainitiative-idcupload).

**`schemas`**. A JSON file with the Mission Impact schema – uses the Frictionless [Table Schema](https://frictionlessdata.io/specs/table-schema/) format.

## How to Use this Repo



## Tests

Run tests with: `python -m pytest`

## Team

The 2019 cohort of Google Fellows devised this ETL pipeline, and the BrightHive engineering team maintains it.

* 