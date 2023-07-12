import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a new Spark session with the specified configuration
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads song_data jsons from S3, extract data to create songs and artists dimensions
    and then write parquet files back to S3

    Keyword arguments:
    spark -- Spark Session Object
    input_data -- AWS S3 Path of Songs Data (JSON)
    output_data -- AWS S3 Path where dimensional tables will be stored (PARQUET)
    """

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.selectExpr('song_id', 'title as song_title', 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id', 
                                  'artist_name as name', 
                                  'artist_location as location', 
                                  'artist_latitude as latitude',
                                  'artist_longitude as longitude'
                                  ).distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Reads log_data jsons from S3, extract data to create users and time Dimensions and songplays Fact table
    and then write parquet files back to S3

    Keyword arguments:
    spark -- Spark Session Object
    input_data -- AWS S3 Path of Songs Data (JSON)
    output_data -- AWS S3 Path where dimensional tables will be stored (PARQUET)
    """

    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col("userId").cast(IntegerType()).alias("user_id"),
                        col("firstName").alias("first_name"),
                        col("lastName").alias("last_name"),
                        col("gender"),
                        col("level")
                       ).where(col("user_id").isNotNull()).dropDuplicates(["user_id"])
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x) / 1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
        
    # extract columns to create time table
    time_table = df.withColumn("hour", hour("start_time")) \
                   .withColumn("day", dayofmonth("start_time")) \
                   .withColumn("week", weekofyear("start_time")) \
                   .withColumn("month", month("start_time")) \
                   .withColumn("year", year("start_time")) \
                   .withColumn("weekday", dayofweek("start_time")) \
                   .select("ts", "start_time", "hour", "day", "week", "month", "year", "weekday") \
                   .drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time/", mode='overwrite', partitionBy=["year","month"])

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,
                     (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration), how='inner') \
                        .select(monotonically_increasing_id().alias("songplay_id"), \
                                col("start_time"), \
                                col("userId").alias("user_id"), \
                                "level","song_id","artist_id", \
                                col("sessionId").alias("session_id"), \
                                "location", \
                                col("userAgent").alias("user_agent")).drop_duplicates()

    songplays_table = songplays_table.withColumn("month",month(col("start_time"))).withColumn("year", year(col("start_time")))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays/", mode='overwrite', partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
