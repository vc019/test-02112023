# Solution for Spark


## Getting Started
The application uses virtual env for dev purposes. Following commands are required to setup a virtual env  
- `python -m venv venv`  
- `source/bin/activate`
- `pip install --upgrade pip`
- `pip install -r requirements.txt`
**Note**: `flake8` and `black` are part of `requirements.txt` and have been applied to the code.

## Configuration
All configuration is maintained in `etl_config.json`. The file currently hardcoded into the main but can be passed as parameter.

## Tests & Coverage
Tests can be executed using `pytest`. Execute the following command to run the tests and check test coverage  
`pytest -v --cov`

## Application Execution
The following command can be used to run the application

### Running locally
`python app.py spark_local`
