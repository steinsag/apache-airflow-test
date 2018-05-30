# Apache Airflow Test

I created this small example to test Python workflow engine [Apache Airflow](https://airflow.apache.org/).


## Requirements

- Python 2 or 3
- [Virtual Environment](https://github.com/pypa/virtualenv)
- MySQL database (tested with MySQL 5.7 on Ubuntu)


## Setup

Those steps need to be done only ones:

- create MySQL DB and user to access it
        
        mysql -u root -p
        
        CREATE DATABSE airflow;
        CREATE USER 'airflow'@'%' IDENTIFIED BY 'airflow';
        GRANT ALL PRIVILEGES ON 'airflow'.* TO 'airflow'@'%' WITH GRANT OPTION;
        FLUSH PRIVILEGES;
        
- test MySQL connection:

        mysql -u airflow -p airflow

- create virtual environment:

        virtualenv env -p python3
        
        or
        
        virtualenv env -p python2

- enter virtual environment:

        source env/bin/activate
        export AIRFLOW_HOME=.
        
- install all dependencies:

        pip install -r requirements.txt
        
- initialize database:

        airflow initdb


## Execute Airflow

Airflow consists of two major components, which must be started in order to execute and manage workflows:

- webserver - web app to manage workflows
- scheduler - component instantiating and executing workflows

In both cases, one needs to first enter the virtual environment:

    source env/bin/activate
    export AIRFLOW_HOME=.

Airflow webserver can be started as follows:

    airflow webserver
    
    
The web app is now available at: http://localhost:8080/
    
Airflow scheduler can be started as follows:

    airflow scheduler


## Trigger New Workflow Instance

This example project defines one workflow type _SAMPLE_WORKFLOW_TYPE_ in _dags/sample_workflow_types.py_.

To instantiate this workflow type, enter virtual environment and trigger workflow instantiation via command line:

    source env/bin/activate
    export AIRFLOW_HOME=.

    airflow trigger_dag SAMPLE_WORKFLOW_TYPE --conf '{"inputAttribute1": "input value 1", "inputAttribute2": "input value 2"}'
