import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import *

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s')

def get_create_spark_session():
    """
    Creates Spark Session
    """
    
    try:
        start_time = datetime.now()
        logging.debug('CREATE_SPARK - START')

        spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        enableHiveSupport().getOrCreate()
    
        elapsed_time = datetime.now() - start_time
        logging.debug(f'CREATE_SPARK - Elapsed Time:[{elapsed_time}]')
        return spark

    except Exception as e:
        logging.exception('CREATE_SPARK - Exception: %s', e, exc_info=True)
        raise

def create_immigration_fact(spark, file_path, output_path):
    """
    Reads parquet provided data on raw_data folder process, extract relevant features
    and then writes into data/parquet folder

    Keyword arguments:
    spark -- Spark Session Object
    file_path -- Path to raw data
    output_data -- Path to write output data (PARQUET)
    """
    try:
        start_time = datetime.now()
        logging.debug('FACT_IMMIGRATION - START')

        df_spark = spark.read.parquet(file_path)
        
        # Select relevant columns
        immigration_fact = df_spark.select(
        'cicid', 'i94yr', 'i94mon', 'i94res', 'i94port',
        'arrdate', 'depdate', 'i94mode', 'i94bir', 'i94visa', 'gender'
        )

        # Convert SAS date to readable date format
        immigration_fact = immigration_fact.withColumn(
            'arrdate',
            F.expr("date_add(to_date('1960-01-01'), int(arrdate))")
        )
        immigration_fact = immigration_fact.withColumn(
            'depdate',
            F.expr("date_add(to_date('1960-01-01'), int(depdate))")
        )

        # Convert FLOAT to INT on specific columns
        immigration_fact = immigration_fact.withColumn("cicid",F.col("cicid").cast(IntegerType())) \
                                            .withColumn("i94yr", F.col("i94yr").cast(IntegerType())) \
                                            .withColumn("i94mon", F.col("i94mon").cast(IntegerType())) \
                                            .withColumn("i94res", F.col("i94res").cast(IntegerType())) \
                                            .withColumn("i94mode", F.col("i94mode").cast(IntegerType())) \
                                            .withColumn("i94bir", F.col("i94bir").cast(IntegerType())) \
                                            .withColumn("i94visa", F.col("i94visa").cast(IntegerType()))
        
        # Write Fact table into Parquet format
        immigration_fact.write.mode('overwrite')\
            .partitionBy('i94yr', 'i94mon').parquet(output_path + "immigration_fact")
        
        elapsed_time = datetime.now() - start_time
        logging.debug(f'FACT_IMMIGRATION - Elapsed Time:[{elapsed_time}]')

    except Exception as e:
        logging.exception('FACT_IMMIGRATION - Exception: %s', e, exc_info=True)
    
def create_dim_demo(spark, file_path, output_path):
    """
    Reads csv provided data on raw_data folder process, extract relevant features
    and then writes into data/parquet folder

    Keyword arguments:
    spark -- Spark Session Object
    file_path -- Path to raw data
    output_data -- Path to write output data (PARQUET)
    """
    try:
        start_time = datetime.now()
        logging.debug('DIM_DEMO - START')

        # Read raw data
        df_demo = spark.read.options(delimiter=';').csv(file_path, header=True)

        # Rename columns
        columns = df_demo.columns
        new_columns = [col.lower().replace(' ', '_').replace('-', '_') for col in columns]
        dim_demo = df_demo.toDF(*new_columns)

        # Select relevant columns
        dim_demo = dim_demo.select(
        'city', 'state_code', 'median_age', 'male_population', 'female_population',
        'total_population', 'foreign_born', 'average_household_size'
        )
        
        # Standartize column value
        dim_demo = dim_demo.withColumn('city', F.upper(F.col('city')))

        # Create composite PK
        dim_demo = dim_demo.withColumn('city_state', F.concat_ws('-',dim_demo.city,dim_demo.state_code))

        # Reorder Columns (DQ purposes)
        dim_demo = dim_demo.select(
        'city_state', 'city', 'state_code', 'median_age', 
        'male_population', 'female_population',
        'total_population', 'foreign_born', 'average_household_size'
        )

        # Drop duplicates (columns selected are not variables)
        dim_demo = dim_demo.dropDuplicates()

        # Write dimension into Parquet format
        dim_demo.write.mode("overwrite").parquet(output_path + 'dim_demo')

        elapsed_time = datetime.now() - start_time
        logging.debug(f'DIM_DEMO - Elapsed Time:[{elapsed_time}]')

    except Exception as e:
        logging.exception('DIM_DEMO - Exception: %s', e, exc_info=True)

def create_dim_port(spark, file_path, output_path):
    """
    Reads csv provided data on raw_data folder process, extract relevant features
    and then writes into data/parquet folder

    Keyword arguments:
    spark -- Spark Session Object
    file_path -- Path to raw data
    output_data -- Path to write output data (PARQUET)
    """

    try:
        start_time = datetime.now()
        logging.debug('DIM_PORT - START')
        # Read raw data
        df_port = spark.read.options(delimiter=',').csv(file_path, header=True)

        # Filter U.S. data
        dim_port = df_port.filter(F.col('iso_country')=='US')

        # Select relevant columns
        dim_port = dim_port.select('ident', 'iso_region', 'municipality')
        
        # Split column value and create new column
        dim_port = dim_port.withColumn('state', F.split(dim_port.iso_region, '-')[1])

        # Drop unused column
        dim_port = dim_port.drop('iso_region')

        # Rename Columns
        columns = ['port_code', 'city', 'state']
        dim_port = dim_port.toDF(*columns)

        # Standartize column value
        dim_port = dim_port.withColumn('city', F.upper(dim_port.city))

        # Create composite PK
        dim_port = dim_port.withColumn('city_state', F.concat_ws('-', dim_port.city, dim_port.state))

        # Write dimension into Parquet format
        dim_port.write.mode("overwrite").parquet(output_path + 'dim_port')

        elapsed_time = datetime.now() - start_time
        logging.debug(f'DIM_PORT - Elapsed Time:[{elapsed_time}]')

    except Exception as e:
        logging.exception('DIM_PORT - Exception: %s', e, exc_info=True)

def create_dim_country(spark, file_path, output_path):
    """
    Reads auxiliary SAS data provided on raw_data folder process, extract relevant features
    and then writes into data/parquet folder

    Keyword arguments:
    spark -- Spark Session Object
    file_path -- Path to raw data
    output_data -- Path to write output data (PARQUET)
    """
    
    try:
        start_time = datetime.now()
        logging.debug('DIM_COUNTRY - START')

        # Read auxiliary reference file
        with open(file_path) as f:
            contents = f.readlines()
            f.close()
        
        # Parser
        countries = {}
        for countrie in contents[10:298]:
            pair = countrie.split('=')
            country_code, country = pair[0].strip(), pair[1].strip().strip("'")
            countries[country_code] = country

        # Create dimension df
        dim_country = spark.createDataFrame(countries.items(), ['country_code', 'country'])

        # Write dimension into Parquet format
        dim_country.write.mode("overwrite").parquet(output_path + 'dim_country')
        
        elapsed_time = datetime.now() - start_time
        logging.debug(f'DIM_COUNTRY - Elapsed Time:[{elapsed_time}]')

    except Exception as e:
        logging.exception('DIM_COUNTRY - Exception: %s', e, exc_info=True)

def create_dim_mode(spark, file_path, output_path):
    """
    Reads auxiliary SAS data provided on raw_data folder process, extract relevant features
    and then writes into data/parquet folder

    Keyword arguments:
    spark -- Spark Session Object
    file_path -- Path to raw data
    output_data -- Path to write output data (PARQUET)
    """
    
    try:
        start_time = datetime.now()
        logging.debug('DIM_MODE - START')

        # Read auxiliary reference file
        with open(file_path) as f:
            contents = f.readlines()
            f.close()
        
        # Parser
        modes = {}
        for mode in contents[973:976]:
            pair = mode.split('=')
            mode_id, mode_type = pair[0].strip(), pair[1].strip().strip("'").strip(";").strip("' ")
            modes[mode_id] = mode_type

        #Create dimension df
        dim_mode = spark.createDataFrame(modes.items(), ['mode_id', 'mode_type'])

        # Write dimension into Parquet format
        dim_mode.write.mode("overwrite").parquet(output_path + 'dim_mode')

        elapsed_time = datetime.now() - start_time
        logging.debug(f'DIM_MODE - Elapsed Time:[{elapsed_time}]')

    except Exception as e:
        logging.exception('DIM_MODE - Exception: %s', e, exc_info=True)

def create_dim_visa(spark, output_path):
    """
    Creates dimension visa based on categories provided

    Keyword arguments:
    spark -- Spark Session Object
    file_path -- Path to raw data
    output_data -- Path to write output data (PARQUET)
    """

    # Define visa type based on auxiliary reference
    try:
        start_time = datetime.now()
        logging.debug('DIM_VISA - START')
        
        visa_data = [
            {'visa_code': 1, 'visa_type': 'Business'},
            {'visa_code': 2, 'visa_type': 'Pleasure'},
            {'visa_code': 3, 'visa_type': 'Student'}
        ]
        
        # Creates visa dimension
        dim_visa = spark.createDataFrame(Row(**x) for x in visa_data)

        dim_visa.write.mode("overwrite").parquet(output_path + 'dim_visa')

        elapsed_time = datetime.now() - start_time
        logging.debug(f'DIM_VISA - Elapsed Time:[{elapsed_time}]')

    except Exception as e:
        logging.exception('DIM_VISA - Exception: %s', e, exc_info=True)

def quality_check(spark, data_path):
    """
    Promotes quality check step on the output parque files

    Keyword arguments:
    spark -- Spark Session Object
    data_path -- Path to data
    """

    try:
        logging.debug('QUALITY_CHECK - START')
        start_time = datetime.now()
        
        for file in os.listdir(data_path):
            if os.path.isdir(data_path + file):
                parquet_path = str(data_path + file)
                df = spark.read.parquet(parquet_path)
                record_num = df.count()
                if record_num <= 0:
                    raise ValueError("This table is empty!")
                else:
                    logging.debug("Table - " + parquet_path.split('/')[-1] + f" - Records: {record_num}")
                
                unique = df.select(df.columns[0]).distinct().count()
                if df.count() > unique:
                    raise ValueError(f"Table - {parquet_path.split('/')[-1]} - Contains Duplicated Unique Key")
                else:
                    logging.debug(f"Table - {parquet_path.split('/')[-1]} - do not contains duplicated Unique Key")
            
            elapsed_time = datetime.now() - start_time
            logging.debug(f'QUALITY_CHECK - Elapsed Time:[{elapsed_time}]')
        
    except Exception as e:
        logging.exception('QUALITY_CHECK - Exception: %s', e, exc_info=True)

def main():
    try:
        logging.debug(f'MAIN - START')
        start_time = datetime.now()
        output_path = 'data/parquet/'
        
        spark = get_create_spark_session()

        create_immigration_fact(spark, 'raw_data/sas_data', output_path)
        create_dim_demo(spark, 'raw_data/us-cities-demographics.csv', output_path)
        create_dim_port(spark, 'raw_data/airport-codes_csv.csv', output_path)
        create_dim_country(spark, 'raw_data/I94_SAS_Labels_Descriptions.SAS', output_path)
        create_dim_mode(spark, 'raw_data/I94_SAS_Labels_Descriptions.SAS', output_path)
        create_dim_visa(spark, output_path)

        quality_check(spark, output_path)

        elapsed_time = datetime.now() - start_time

        logging.debug(f'MAIN - Elapsed Time: [{elapsed_time}]')
    
    except Exception as e:
        logging.exception('MAIN - Exception: %s', e, exc_info=True)

if __name__ == '__main__':
    main()