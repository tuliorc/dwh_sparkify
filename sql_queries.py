import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""        
CREATE TABLE IF NOT EXISTS staging_events (
    id_event INTEGER IDENTITY(0, 1) PRIMARY KEY,
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR,
    itemInSession INTEGER, 
    lastName VARCHAR,
    length DOUBLE PRECISION,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR, 
    sessionId BIGINT,
    song VARCHAR, 
    status INTEGER,
    ts VARCHAR,
    userAgent TEXT,       
    userId VARCHAR
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR NOT NULL,    
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR,
    artist_name VARCHAR, 
    song_id VARCHAR NOT NULL PRIMARY KEY,
    title VARCHAR,
    duration DOUBLE PRECISION,
    year INTEGER
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time VARCHAR,
    user_id VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id VARCHAR,
    level VARCHAR,
    location VARCHAR,
    user_agent TEXT
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users (
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR,
    level VARCHAR
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR, 
    year INTEGER, 
    duration DOUBLE PRECISION
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR, 
    location VARCHAR, 
    latitude DOUBLE PRECISION, 
    longitude DOUBLE PRECISION
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time (
    start_time VARCHAR PRIMARY KEY,
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER, 
    weekday INTEGER
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    JSON {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])
# The COPY function only parses the first-level JSON data structures to columns in target table by matching each name. 
# For this operation to be successful, we are required to specify a JSON path to tell the target table to follow the same order as found in the JSON path. Otherwise, COPY loads the fields automatically and assumes source and target are already in the same order. 

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])
# The COPY function only parses the first-level JSON data structures to columns in target table by matching each name. 
# For this operation to be successful, we are required to specify a JSON path to tell the target table to follow the same order as found in the JSON path. Otherwise, COPY loads the fields automatically and assumes source and target are already in the same order. 

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay (start_time, user_id, song_id, artist_id, session_id, level, location, user_agent)
SELECT
    e.ts,
    e.userId,
    s.song_id,
    s.artist_id,
    e.sessionId,
    e.level,
    e.location,
    e.userAgent
FROM staging_events e JOIN staging_songs s ON e.song = s.title
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO dim_users (first_name, last_name, gender, level)
SELECT DISTINCT
    firstName,
    lastName,
    gender,
    level
FROM
    staging_events
WHERE
    page = 'nextSong';
""")

song_table_insert = ("""
INSERT INTO dim_songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM
    staging_songs;
""")


artist_table_insert = ("""
INSERT INTO dim_artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM
    staging_songs;
""")

time_table_insert = ("""
INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
SELECT start_time,
    extract(hour from start_time) h,
    extract(day from start_time) d,
    extract(week from start_time) w,
    extract(month from start_time) m,
    extract(year from start_time) y,
    extract(weekday from start_time) wd
FROM (SELECT DISTINCT TIMESTAMP 'epoch' + start_time/1000 * INTERVAL '1 second' as start_time FROM fact_songplay);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
