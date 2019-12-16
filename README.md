# GoodwillDataInitiative ETL

`goodwilldatainitiative-etl` contains a collection of scripts that extract Mission Impact (MI) data from local Goodwills, transform the data into the Goodwill International (GII) MI schema, and load the data into GII Gateway. Two tools use these scripts: an [Airflow tool](https://github.com/GIIMSC/goodwilldatainitiative-airflow), which runs the automated pull/push solution, and a [Flask app](https://github.com/GIIMSC/goodwilldatainitiative-idcupload), which provides a user interface for manual uploads.

This code comes from [a larger project repo, which has been archived](https://github.com/GIIMSC/GoodwillDataInitiative).

## Code organization

The `etl` directory has three subdirectories.

**`helpers`**. The ETL business logic. 

**`pipeline`**. A standalone version of the transformation process. `simple_pipeline.py` runs the "T" of the ETL pipeline: it accepts local Goodwill credentials and MI data, and it returns data in the GII MI schema. This script, in particular, serves the [Flask app](https://github.com/GIIMSC/goodwilldatainitiative-idcupload).

**`schemas`**. A JSON file with the Mission Impact schema – uses the Frictionless [Table Schema](https://frictionlessdata.io/specs/table-schema/) format.

## How to Use this Repo

This repo comes with a handy `setup.py`, which makes easy the installation of `goodwilldatainitiative-etl` and the required dependencies. You can install it directly from Github using one of the following approaches:

```
# Install it directly in your virtual env
pip install git+https://github.com/GIIMSC/goodwilldatainitiative-etl.git
```

```
# Add this line to your requirements.txt 
git+https://github.com/GIIMSC/goodwilldatainitiative-etl.git

# Install it
pip install requirements.txt
```

```
# Add this line to your Pipfile
etl = {editable = true,ref = "master",git = "https://github.com/GIIMSC/goodwilldatainitiative-etl.git"}

# Install it, and update the Pipfile.lock, which pins the repo to the latest commit
pipenv install
```

You can import the etl module and/or specific methods the usual way, for example:

```python
from etl.helpers import drive, gateway, table_schema
from etl.pipeline import simple_pipeline
```

## Tests, Linting, and CircleCI

This repo contains dozens of unit tests written with `pytest`. The test files reside in the subdirectories of the code that they test. CircleCI runs the tests on each push, but you can run them yourself with:

`python -m pytest`

CircleCI also runs [Black](https://github.com/psf/black) ("the uncompromising Python code formatter"). If you need to reformat new code, then run:

```
# Check for issues
black --check etl

# Reformat code
black {path/to/code/that/needs/reformatting}
```

## Team

The 2019 cohort of Google Fellows devised this ETL pipeline, and the BrightHive engineering team maintains it.

* [Dana Hoffman](https://github.com/danawillow) (Google)
* [Roshan Agrawal](https://github.com/roshcagra) (Google)
* [John Han](https://github.com/hanjohn) (Google)
* [Joel Jacobs](https://github.com/jacobsjmd) (Google)
* [Tom Plagge](https://github.com/tplagge) (BrightHive)
* [Regina Compton]()