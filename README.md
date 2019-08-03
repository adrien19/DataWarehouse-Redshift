# Project - AWS Data Warehouse for Sparkify

Case: A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides on AWS S3 in JSON logs on user activity on the app, as well as with JSON metadata on the songs in their app also on AWS S3.

The project creates a data warehouse in AWS using Redshift to build an ETL pipeline. A star schema was used where fact and dimension tables have been defined for a particular analytic focus. The ETL pipeline is used to transfer data from AWS S3 into the staging tables in Redshift cluster, then transformation is applied before loading the data into tables for analytic team to use.


## Getting Started

The details below will get you a summary of the project's data watehouse for development and testing purposes.

### Prerequisites

You need to create a Redshift cluster and include the information in dwh.cfg for accessing this cluster in aws. Some information on where the data resides in AWS S3 are provided in the dwh.cfg. Before running etl.py, make sure create_tables.py has been run at least once to create the reshift cluster's tables.


### Installing

All files can be downloaded and stored on local machine. From there, the create the database by running create_tables.py. Then you can run the etl.py. etl.py executes queries that reads and copy a single file from song_data and log_data and loads the data into staging tables. etl.py also executes queries that insert data from staging tables into defined tables for analytics team. This was created based on your work in the ETL notebook.

files included:
* create_tables.py
* etl.py
* sql_queries.py
* README.md

## Running the tests


Before running the code, create a `dwh.cfg` file and include the following information:

```
# Amazon Redshift credentials
[CLUSTER]
HOST=''
DB_NAME=''
DB_USER=''
DB_PASSWORD=''
DB_PORT=5439

[IAM_ROLE]
ARN=''

[S3]
LOG_DATA='s3://...' # where log files are stored
LOG_JSONPATH='s3://...' # Where the log json path is stored
SONG_DATA='s3://...' # Where the song data files are stored
```

Run create_tables, then etl.py. to confirm the creation of your tables with the correct columns. Logon to AWS and go to Redshift cluster. Run test queries on Query Editor located just below "Clusters".

Example query:

 SELECT * FROM users WHERE users.level='free' LIMIT 5;


## Authors

* **Adrien Ndikumana** - [adrien19](https://github.com/adrien19)


## Acknowledgments

* Inspiration from Udacity Team
