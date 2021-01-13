import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events 
(artist varchar, auth varchar, firstName varchar, gender char, 
itemInSsession integer, lastName varchar,
length float, level varchar, location varchar, method varchar,
page varchar, registration float, session_id integer, song varchar,
status integer, ts BIGINT, user_agent varchar, user_id INTEGER);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs 
(num_songs integer, artist_id varchar, artist_latitude float, artist_longitude float,
artist_location varchar, artist_name varchar, song_id varchar, title varchar,
duration float, year integer );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id INTEGER IDENTITY (1,1) PRIMARY KEY,
start_time TIMESTAMP, user_id INTEGER NOT NULL, level varchar NOT NULL, 
song_id varchar NOT NULL, artist_id varchar NOT NULL, session_id integer,
location varchar, user_agent varchar)
DISTSTYLE KEY
     DISTKEY (start_time)
     SORTKEY (start_time);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY,
first_name varchar,last_name varchar, gender CHAR(1),
level varchar)
SORTKEY (user_id);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR PRIMARY KEY, 
title varchar, artist_id varchar NOT NULL, year integer,
duration float NOT NULL)
SORTKEY (song_id);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
(artist_id VARCHAR PRIMARY KEY, name varchar NOT NULL, 
location varchar, lattitude float, longitude float)
SORTKEY (artist_id);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
(start_time TIMESTAMP PRIMARY KEY,
hour integer NOT NULL,day integer NOT NULL, week integer NOT NULL,
month integer NOT NULL, year integer NOT NULL, weekday VARCHAR NOT NULL)
DISTSTYLE KEY
DISTKEY (start_time)
SORTKEY (start_time);
""")

# STAGING TABLES
staging_events_copy = (""" COPY staging_events FROM {} iam_role {} FORMAT AS json {};
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""COPY staging_songs FROM {} iam_role {} FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES: Populate create tables with data from staging tables
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id,
level, song_id, artist_id, session_id, location, user_agent) 
SELECT DISTINCT
     TIMESTAMP 'epoch' + (se.ts/1000) * INTERVAL '1 second' as start_time,
     se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id,
     se.location, se.user_agent
     FROM staging_songs ss
     INNER JOIN staging_events se
     ON (ss.title = se.song AND se.artist = ss.artist_name)
     WHERE se.ts is NOT NULL;
""")
'''
I didn't include ss.duration = se.length as extra condition for the join clause as we are not
sure of the column definition for length. It might be the length of time spent by the user
listenting to the music
'''

user_table_insert = ("""INSERT INTO users
SELECT DISTINCT user_id, firstName, lastName, gender, level
FROM staging_events
WHERE user_id IS NOT NULL
AND page = 'NextSong';

""")

song_table_insert = ("""INSERT INTO songs
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude,
artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;

""")

time_table_insert = ("""INSERT INTO time 
SELECT DISTINCT
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_events
WHERE ts is NOT NULL;

""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
