# Project: Data Pipelines with Airflow

## Introduction
This project is focused on create an ETL pipeline using Python to create a database that will be used to analyze data that have been collected from users activity on a music streamming app.
<br />As requested, a NoSQL Database (Apache Cassandra) was chosen to store all the information, where analytics team will be able to create queries and answer business questions.

## Project Dataset
All the data files are located within the ```event_data``` directory. The CSV files are partitioned by date as shown below.

```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```
