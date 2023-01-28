import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from tools import read_config, print_status


def create_spark_session():
    """
    Description: This function creates a Spark session

    Arguments:
        None

    Returns:
        spark: Spark session
    """

    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .config('mapreduce.fileoutputcommitter.algorithm.version', '2') \
        .config('spark.sql.parquet.fs.optimized.committer.optimization-enabled', 'true') \
        .config('spark.sql.broadcastTimeout', '-1') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function reads the json-files from song data,
                 processes them, and writes song table and artist table to AWS S3

    Arguments:
        spark: Spark session
        input_data: Path to the folder of the input data
        output_data: Path to the folder of the output data

    Returns:
        None
    """

    # get filepath to song data file
    song_data_inpath = os.path.join(input_data, 'song_data/*/*/*/*.json')
    #song_data_inpath = os.path.join(input_data, 'song_data/A/A/A/*.json')
    
    # read song data file
    df = spark.read.json(path=song_data_inpath)
    print_status('process_song_data', 'song data loaded')

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    print_status('process_song_data', 'songs_table select completed')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table_outpath = os.path.join(output_data, 'song_table.parquet')
    songs_table.write.partitionBy('year','artist_id').parquet(songs_table_outpath, 'overwrite')
    print_status('process_song_data', 'songs_table written to S3')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').distinct()
    print_status('process_song_data', 'artists_table select completed')

    # write artists table to parquet files
    artists_table_outpath = os.path.join(output_data, 'artists_table.parquet')
    artists_table.write.parquet(artists_table_outpath, 'overwrite')
    print_status('process_song_data', 'artists_table written to S3')

    # Create a view and cache this DataFrame to be able to access to this later
    df.createOrReplaceTempView('song_df')
    spark.table('song_df')
    spark.table('song_df').cache
    spark.table('song_df').count


def process_log_data(spark, input_data, output_data):
    """
    Description: This function reads the json-files from log data,
                 processes them, and writes users table,
                 time table and songplays table to AWS S3

    Arguments:
        spark: Spark session
        input_data: Path to the folder of the input data
        output_data: Path to the folder of the output data

    Returns:
        None
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(path=log_data)
    print_status('process_log_data', 'log data loaded')
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()
    print_status('process_log_data', 'users_table select completed')

    # write users table to parquet files
    users_table_outpath = os.path.join(output_data, 'users_table.parquet')
    users_table.write.parquet(users_table_outpath, 'overwrite')
    print_status('process_log_data', 'users_table written to S3')

    # create timestamp column from original timestamp column
    get_timestamp_udf = udf(lambda x: int(x / 1000))
    df = df.withColumn('timestamp', get_timestamp_udf('ts').cast('Integer'))
    
    # create datetime column from original timestamp column
    get_datetime_udf = udf(lambda x: str(datetime.fromtimestamp(x)))
    df = df.withColumn('start_time', get_datetime_udf('timestamp').cast('Timestamp'))
    
    # extract columns to create time table
    time_table = df.select('start_time') \
                   .withColumn('hour', hour('start_time').cast('Integer')) \
                   .withColumn('day', dayofmonth('start_time').cast('Integer')) \
                   .withColumn('week', weekofyear('start_time').cast('Integer')) \
                   .withColumn('month', month('start_time').cast('Integer')) \
                   .withColumn('year', year('start_time').cast('Integer')) \
                   .withColumn('weekday', date_format('start_time', 'u').cast('Integer')) \
                   .distinct()
    print_status('process_log_data', 'time_table select completed')

    # write time table to parquet files partitioned by year and month
    time_table_outpath = os.path.join(output_data, 'time_table.parquet')
    time_table.write.partitionBy('year','month').parquet(time_table_outpath, 'overwrite')
    print_status('process_log_data', 'time_table written to S3')

    # read in song data to use for songplays table
    song_df = spark.sql('SELECT DISTINCT song_id, title, artist_id, artist_name FROM song_df')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table_join = df.join(song_df, (df.song==song_df.title) & (df.artist==song_df.artist_name), how='left')
    songplays_table = songplays_table_join.select(monotonically_increasing_id().alias('songplay_id'),
                                                  'start_time',
                                                  'userId',
                                                  'level',
                                                  'song_id', 
                                                  'artist_id', 
                                                  'sessionId', 
                                                  'location', 
                                                  'userAgent')

    # write songplays table to parquet files
    songplays_table_outpath = os.path.join(output_data, 'songplays_table.parquet')
    songplays_table.write.parquet(songplays_table_outpath, 'overwrite')


def main():
    """
    Description: This main function specifies the input_data and output_data. 
                 It also triggers the functions process_song_data and process_log_data.
    
    Arguments:
        None
        
    Returns:
        None
    """
    output_data = read_config('dl.cfg')
    spark = create_spark_session()
    input_data = 's3a://udacity-dend'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
