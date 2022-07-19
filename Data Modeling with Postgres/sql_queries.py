# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP REFERENCES time (start_time),
    user_id INT REFERENCES users (user_id),
    level VARCHAR,
    song_id VARCHAR REFERENCES songs (song_id),
    artist_id VARCHAR REFERENCES artists (artist_id),
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARI KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR
)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id INT PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR REFERENCES artists (artist_id),
    year INT,
    duration FLOAT
)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
)
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays () VALUES ()
""")

user_table_insert = ("""INSERT INTO users () VALUES ()
""")

song_table_insert = ("""INSERT INTO songs () VALUES ()
""")

artist_table_insert = ("""INSERT INTO  artists () VALUES ()
""")


time_table_insert = ("""INSERT INTO time () VALUES ()
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create,
                        song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
