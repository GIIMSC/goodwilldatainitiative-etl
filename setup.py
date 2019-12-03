import setuptools

# Most package versions are arbitrarily constrained to be more recent than
# a version known to work in development.
REQUIRED_PACKAGES = [
    # Performance improvement for fuzzywuzzy in python-Levenshtein.
    "fuzzywuzzy==0.17.0",
    "python-Levenshtein==0.12.0",
    # Frictionless data deps.
    "great_expectations==0.4.5",
    "tableschema==1.3.0",
    # Needed for GCS client library.
    "google-cloud-core==1.0.3",
    "google-cloud-storage==1.23.0",
    # The Pandas version is actually critical; the nullable int type
    # `pd.Int64Dtype()` was recently added in version 0.24.0.
    "pandas==0.25.2",
    # Required to talk to CaseWorthy API:
    "pycrypto==2.6.1",
    "pyaes==1.6.1",
    "zeep==3.4.0",
    # Old versions of setuptools don't work well with pytest.
    "setuptools>=40.6.3",
    # Others:
    "tabulate==0.8.5",
    "apache-airflow>=1.10.2",
    "google-api-python-client==1.7.8",
    "jinja2==2.10.3",
    "pysftp==0.2.9",
    "requests==2.22.0",
    "us==1.0.0",
    "simple-salesforce==0.74.2",
    "pendulum==1.4.4",
    "pytest-runner==5.1",
    "psycopg2-binary==2.8.2",
    "Flask==1.1.1",
    "Werkzeug==0.15.4",
]

setuptools.setup(
    name="goodwilldatainitiative_etl",
    description="Code and docs for ETLs plumbing data from local Goodwills to "
    "Goodwill Industries International, as part of the Google/Goodwill "
    "fellowship program.",
    author="Google",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
    ],
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    # setup_requires=["pytest-runner==5.1"],
    tests_require=["pytest"],
    # Use pkg_resources to point at files without depending on relative paths.
    package_data={
        "etl.helpers": ["**.csv"],
        "etl.schemas": ["*.json"],
        "testfiles": ["**.csv"],
    },
)
