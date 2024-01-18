# Project: Data Pipelines with Apache Airflow

## Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Dataset
Datasets
For this project, the data were stored on s3:

```Log data: s3://udacity-dend/log_data```

```Song data: s3://udacity-dend/song_data```

## How to Run
1. ```Create an IAM User in AWS```
    - The user must have the following permissions:
      - AdministratorAccess
      - AmazonRedshiftFullAccess
      - AmazonS3FullAccess   
2. ```Create Redshift Cluster```
    - Input Admin user name
    - Input Admin user password
    - Enable publicly accessible setting
    - Enable Enhanced VPC Routing (Network and Security settings)
    - Add Inbound Rule to the VPC security group
      - Type = Custom TCP
      - Port range = 0 - 5500
      - Source = Anywhere-iPv4   
3. ```Connect Airflow and AWS```
    - Add Airflow Connections
      - Conn Id: Enter aws_credentials
      - Conn Type: Enter Amazon Web Services
      - Login: Enter your Access key ID from the IAM User credentials
      - Password: Enter your Secret access key from the IAM User credentials 
4. ```Connect Airflow to the AWS Redshift Cluster```
    - Add Airflow Connections
      - Conn Id: Enter redshift
      - Conn Type: Enter Postgres
      - Host: RDS endpoint
      - Schema: dev. This is the Redshift database you want to connect to
      - Login: awsuser
      - Password: Password you created when launching your Redshift cluster
      - Port: Enter 5439 
5. ```Trigger DAG```
6. ```Delete Redshift Resources```
    - Delete all the resources created to run the workflow in order to prevent any extra fees from AWS.
