# Capstone Project

## Introduction

## Project Datasets

### [I-94 Immigration Data](https://www.trade.gov/i-94-arrivals-historical-data)
This dataset comes from the US National Tourism and Trade Office and contains information about immigrants in the U.S.
A data dictionary is included in the workspace on the following path (dictionary).

### [Airport Code](https://datahub.io/core/airport-codes#data)
This dataset contains airport codes and corresponding cities.

### [U.S. City Demographic Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)
This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000.
The data comes from the US Census Bureau's 2015 American Community Survey.

## [Auxiliary Data](https://github.com/willytakasawa/data-engineering-nanodegree/blob/master/Capstone%20Project/raw_data/I94_SAS_Labels_Descriptions.SAS)


## Data Schema
### Schema for U.S. Immigration Analysis
This project uses star schema with one fact table and four dimension tables to optimize ad-hoc queries on U.S. immigration events.

#### Fact Table
1. **immigration_fact** - records of all U.S. immigration events from I94
    - *cicid* - Primary Key
    - *i94yr* - event year
    - *i94mon* - event month 
    - *i94res* - country code for immigration
    - *i94port* - port code admitted through
    - *arrdate* - arrival date
    - *i94mode* - model of transportation
    - *depdate* - departure date
    - *i94bir* - age
    - *i94visa* - visa type
    - *gender* - gender
#### Dimension Tables
2. **dim_mode** - model of transportation
    - *mode_id*
    - *mode* - mode description
3. **dim_visa** - visa type
    - *visa_id*
    - *visa_type* - visa category
4. **dim_country** - country information
    - *country_code*
    - *country_name*
5. **dim_port** - port information
    - *port_code*
    - *city*
    - *state*
    - *city_state* Foreign Key from dim_demo
6. **dim_demo**
     - *city_state* Concat. of city and state (Primary Key)
    - *city*
    - *state*
    - *median_age*
    - *male_population*
    - *female_population*
    - *total_population*
    - *foreign_born*
    - *avg_household_size*    

![](https://github.com/willytakasawa/data-engineering-nanodegree/blob/master/Capstone%20Project/img/dend-udacity.png)

## Project Structure
The workflow which reads raw data from auxiliary files, processes them to create the dimensional model and loads target tables back to the output folder
into analytical structure can be found in [pipeline.py](https://github.com/willytakasawa/data-engineering-nanodegree/blob/master/Capstone%20Project/pipeline.py)
script.


## How to Run
1. ```pipeline.py```
    - Reads raw data from files provided
    - Processes them using PySpark to create tables based on analytical structure (multidimensional-schema)
    - Loads the dimensions and fact as parquet files

## Next Steps
1. Analyze data
2. Copy Parquet Data into Amazon Redshift / GCP BigQuery
3. Construct Airflow DAG
4. Insert more relevant information
