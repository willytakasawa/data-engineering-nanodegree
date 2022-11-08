import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (
    """
    CREATE TABLE IF NOT EXISTS staging_events (
    abc INT,
    
    )
    """
)

staging_songs_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_songs (
    abc INT,
    
    )
    """
)

songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songplay (
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
    CREATE TABLE IF NOT EXISTS user (
    user_id PRIMARY KEY,
    first_name VARCHAR(),
    last_name VARCHAR(),
    gender CHAR(1),
    level CHAR(4)
    )
    """
)

song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS song (
    song_id PRIMARI KEY,
    title VARCHAR(),
    artist_id VARCHAR() NOT NULL REFERENCES artist(artist_id),
    year INT,
    duration FLOAT
    """
)

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artist (
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
    CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP,
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

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
