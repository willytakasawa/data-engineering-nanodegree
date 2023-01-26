import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fct_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR(),
    auth VARCHAR(),
    firstName VARCHAR(),
    gender CHAR(1),
    itemInSession INT,
    lastName VARCHAR(),
    length FLOAT,
    level CHAR(4),
    location VARCHAR(),
    method VARCHAR(),
    page VARCHAR(),
    registration FLOAT,
    sessionId INT,
    song VARCHAR(),
    status INT,
    ts INT,
    userAgent VARCHAR(),
    userId INT
    )
    """
)

staging_songs_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR(),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(),
    artist_name VARCHAR(),
    song_id VARCHAR() PRIMARY KEY,
    title VARCHAR(),
    duration FLOAT
    year INT
    )
    """
)

songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS fct_songplays (
    songplay_id PRIMARY KEY IDENTITY(0,1),
    start_time TIMESTAMP,
    user_id INT REFERENCES user(user_id),
    level CHAR(4),
    song_id VARCHAR REFERENCES song(song_id),
    artist_id VARCHAR REFERENCES artist(artis_id),
    session_id INT NOT NULL,
    location VARCHAR(),
    user_agent VARCHAR()
    )
    """
)

user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS dim_users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR(),
    last_name VARCHAR(),
    gender CHAR(1),
    level CHAR(4)
    )
    """
)

song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS dim_songs (
    song_id VARCHAR() PRIMARY KEY,
    title VARCHAR(),
    artist_id VARCHAR() NOT NULL REFERENCES artist(artist_id),
    year INT,
    duration FLOAT
    """
)

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS dim_artists (
    artist_id PRIMARY KEY,
    name VARCHAR(),
    location VARCHAR(),
    lattitude FLOAT,
    longitude FLOAT
    )
    """
)

time_table_create = (
    """
    CREATE TABLE IF NOT EXISTS dim_time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
    )
    """
)

# STAGING TABLES

staging_events_copy = (
    """
    copy staging_events
    from {bucket}
    credentials 'aws_iam_role={iam_role}'
    region 'us-west-2'
    format as JSON {json_path}
    timeformat as 'epochmillisecs'
    """
).format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = (
    """
    copy staging_songs
    from {bucket}
    credentials 'aws_iam_role={iam_role}'
    region 'us-west-2'
    format as JSON 'auto'
    """
).format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO fct_songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT(
        TIMESTAMP('epoch' + (e.ts/1000 * INTERVAL '1 second'))
        e.userId,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
        )
    FROM staging_events e
    LEFT JOIN staging_songs s ON
        e.artist = s.artist_name AND e.song = s.title
    """
)

user_table_insert = (
    """
    INSERT INTO dim_users(user_id, first_name, last_name, gender, level)
    SELECT(
        DISTINCT(userId),
        firstName,
        lastName,
        gender,
        level
    )
    FROM staging_events
    """
)

song_table_insert = (
    """
    INSERT INTO dim_songs(song_id, title, artist_id, year, duration)
    SELECT(
        DISTINCT(song_id),
        title,
        artist_id,
        year,
        duration
    )
    FROM staging_songs
    """
)

artist_table_insert = (
    """
    INSERT INTO dim_artists(artist_id, name, location, lattitude, longitude)
    SELECT(
        DISTINCT(artist_id),
        artist_name,
        location,
        latitude,
        longitude
    )
    FROM staging_songs
    """
)

time_table_insert = (
    """
    INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
    WITH tmp AS (SELECT(TIMESTAMP('epoch' + (ts/1000 * INTERVAL '1 second') AS ts FROM staging_event)))
    SELECT DISTINCT(
        ts,
        EXTRACT(hour FROM ts),
        EXTRACT(day FROM ts),
        EXTRACT(week FROM ts),
        EXTRACT(month FROM ts),
        EXTRACT(year FROM ts),
        EXTRACT(weekday FROM ts),
    )
    FROM tmp
    """
)

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
