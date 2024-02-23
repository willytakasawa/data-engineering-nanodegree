from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import *

def get_create_spark_session():
    """
    Creates Spark Session
    """

    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()

    return spark

def create_immigration_fact(spark, file_path, output_path):
    """
    
    """

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
    
def create_dim_demo(spark, file_path, output_path):
    """
    
    """

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

def create_dim_port(spark, file_path, output_path):
    """
    
    """

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

def create_dim_country(spark, file_path, output_path):
    """
    
    """

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

def create_dim_mode(spark, file_path, output_path):
    """
    
    """

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


def create_dim_visa(spark, output_path):
    """
    
    """

    # Define visa type based on auxiliary reference
    visa_data = [
        {'visa_code': 1, 'visa_type': 'Business'},
        {'visa_code': 2, 'visa_type': 'Pleasure'},
        {'visa_code': 3, 'visa_type': 'Student'}
    ]
    
    # Creates visa dimension
    dim_visa = spark.createDataFrame(Row(**x) for x in visa_data)

    dim_visa.write.mode("overwrite").parquet(output_path + 'dim_visa')

"""def quality_check(spark):
    list_dir = ['dim_country', 'dim_demo', 'dim_port', 'dim_mode', 'dim_visa', 'immigration_fact']
    for file_dir in list_dir:
        #if file_dir.is_dir():
        path = str(file_dir)
        df = spark.read.parquet(path)
        record_num = df.count()
        if record_num <= 0:
            raise ValueError("This table is empty!")
        else:
            print("Table - " + path.split('/')[-1] + f" - Records: {record_num}")

    for file_dir in list_dir:
        path = str(file_dir)
        df = spark.read.parquet(path)
        unique = df.select(df.columns[0]).distinct().count()
        if df.count() > unique:
            raise ValueError(f"Table - {path} - Contains Duplicated Unique Key")
        else:
            print(f"Table - {path} - do not contains duplicated Unique Key")"""

def main():
    output_path = 'data/parquet/'
    spark = get_create_spark_session()

    create_immigration_fact(spark, 'raw_data/sas_data', output_path)
    create_dim_demo(spark, 'raw_data/us-cities-demographics.csv', output_path)
    create_dim_port(spark, 'raw_data/airport-codes_csv.csv', output_path)
    create_dim_country(spark, 'raw_data/I94_SAS_Labels_Descriptions.SAS', output_path)
    create_dim_mode(spark, 'raw_data/I94_SAS_Labels_Descriptions.SAS', output_path)
    create_dim_visa(spark, output_path)

if __name__ == '__main__':
    main()