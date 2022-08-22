# Project: Data Modeling with Apache Cassandra

## Introduction
This project is focused on create an ETL pipeline using Python to create a database that will be used to analyze data that have been collected from users activity on a music streamming app.
<br />As requested, a NoSQL Database (Apache Cassandra) was chosen to store all the information, where analytics team will be able to create queries and answer business questions.

## Project Dataset
All the data files are located within the ```event_data``` directory. The CSV files are partitioned by date as shown below.

```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

The CSV files consists in user activity data from the music streamming app.

Based on the ETL proccess, the main information that was extracted from the CSV files were:

- artist
- firstName of user
- gender of user
- item number in session
- last name of user
- song length
- level (paid or free song)
- location of the user
- sessionId
- song title
- userId

That information was used to create a single CSV file titled ```event_datafile_new.csv``` which will be used to load data in corresponding tables.

## How to run
1. Clone this Repository
2. ```docker-compose -f "cassandra-docker-compose.yml" up -d --build```
    - builds a container running Apache Cassandra
3. [Project_1B_ Project_Template.ipynb](https://github.com/willytakasawa/data-engineering-nanodegree/blob/master/Data%20Modeling%20with%20Apache%20Cassandra/Project_1B_%20Project_Template.ipynb)
    - ETL pipeline
