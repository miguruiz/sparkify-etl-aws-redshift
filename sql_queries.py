import configparser

"""
The following script contains the SQL queries executed by create_table.py and etl.py
"""
# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA           = config.get("S3","LOG_DATA")
LOG_JSONPATH       = config.get("S3","LOG_JSONPATH")
SONG_DATA          = config.get("S3","SONG_DATA")
ZONE               = config.get("PARAMS","ZONE") 
ANR                = config.get("OTHER","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE STAGING TABLES

staging_events_table_create= (""" CREATE TABLE events_staging (
    artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender TEXT,
    iteminSession INT,
    lastName TEXT,
    length DECIMAL,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration FLOAT,
    sessionid INT,
    song TEXT,
    status INT,
    ts BIGINT,
    userAgent TEXT,
    userid INT
)
""")
    
staging_songs_table_create = (""" CREATE TABLE songs_staging (
    num_songs INT,
    artist_id TEXT,
    artist_latitude TEXT,
    artist_location TEXT,
    artist_longitude TEXT,
    artist_name TEXT,
    song_id TEXT,
    title TEXT,
    duration DECIMAL,
    year INT)
""")


# CREATE FINAL TABLES sortkey distkey

songplay_table_create = (""" CREATE TABLE songplays (
    songplay_id INT IDENTITY(0,1),
    start_time TIMESTAMP NOT NULL sortkey,
    user_id INT distkey,
    level TEXT NOT NULL,
    song_id TEXT,
    artist_id TEXT,
    session_id INT NOT NULL,
    location TEXT,
    user_agent TEXT
)
""")

user_table_create = (""" CREATE TABLE users(
    user_id INT UNIQUE NOT NULL sortkey,
    first_name TEXT,
    last_name TEXT,
    gender VARCHAR(1),
    level TEXT NOT NULL
    )
""")

song_table_create = ("""CREATE TABLE songs (
    song_id TEXT UNIQUE NOT NULL sortkey,
    title TEXT NOT NULL,
    artist_id TEXT,
    year INT,
    duration INT
    )
""")

artist_table_create = (""" CREATE TABLE artists (
    artist_id TEXT UNIQUE NOT NULL sortkey,
    name TEXT NOT NULL,
    location TEXT,
    lattitude TEXT,
    longitude TEXT
)
""")

time_table_create = (""" CREATE TABLE time (
    start_time TIMESTAMP UNIQUE NOT NULL sortkey,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
)
""")

# COPY TO STAGING TABLES

staging_events_copy = (f"""
    copy events_staging
    from '{LOG_DATA}' 
    iam_role '{ANR}'
    JSON '{LOG_JSONPATH}'
    region '{ZONE}';
""")

staging_songs_copy = (f"""
    copy songs_staging from '{SONG_DATA}'
    credentials 'aws_iam_role={ANR}'
    json 'auto'
    region '{ZONE}';
""")


# INSERT TO FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(SELECT
    date_add('ms',ts,'1970-01-01'), 
    e.userid,  
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionid,
    e.location,
    e.userAgent
FROM events_staging e
LEFT JOIN songs_staging s
ON e.artist = s.artist_name 
AND e.song = s.title
AND e.length = s.duration 
);
""")


# Using window function to ensure userId, songs_id, and artists_id are unique 

user_table_insert = ("""
INSERT INTO users (
    SELECT  userid, 
            firstName, 
            lastName, 
            gender, 
            level
    FROM (
        SELECT DISTINCT
            userid, 
            firstName, 
            lastName, 
            gender, 
            level,
            rank() OVER (PARTITION BY userid ORDER BY ts,firstName,lastName,gender,level DESC) AS rank  
        FROM events_staging) 
    WHERE rank = 1 and userid is not null
    )
""")
    
song_table_insert = ("""
INSERT INTO songs
( SELECT  song_id, 
            title, 
            artist_id, 
            year, 
            duration
    FROM (
        SELECT DISTINCT
            song_id, 
            title, 
            artist_id, 
            year, 
            duration,
            rank() OVER (PARTITION BY song_id ORDER BY year,duration,title,duration  DESC) AS rank       
        FROM songs_staging) 
    WHERE rank = 1 and song_id is not null);
""")

artist_table_insert = ("""
INSERT INTO artists(
    SELECT  artist_id, 
            artist_name, 
            artist_location, 
            artist_latitude, 
            artist_longitude
    FROM (
        SELECT DISTINCT
            artist_id, 
            artist_name, 
            artist_location, 
            artist_latitude, 
            artist_longitude,
            rank() OVER (PARTITION BY artist_id ORDER BY year,artist_location,artist_name,artist_latitude,artist_longitude  DESC) AS rank       
        FROM songs_staging) 
    WHERE rank = 1
)""")

time_table_insert = ("""
INSERT INTO time 
( 
SELECT 
    DISTINCT date_add('ms',ts,'1970-01-01') as start_time, 
    EXTRACT(hour from date_add('ms',ts,'1970-01-01')) as hour,
    EXTRACT(day from date_add('ms',ts,'1970-01-01')) as day,
    EXTRACT(week from date_add('ms',ts,'1970-01-01')) as week,
    EXTRACT(month from date_add('ms',ts,'1970-01-01')) as month,
    EXTRACT(year from date_add('ms',ts,'1970-01-01')) as year,
    EXTRACT(weekday from date_add('ms',ts,'1970-01-01')) as weekday
FROM events_staging);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
