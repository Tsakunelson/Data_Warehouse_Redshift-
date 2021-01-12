# Project Motivation
A music startup is starting to experience growth and has decided to move thier data to the cloud. The collected data takes the form of JSON files from user activity on the music app and a second set of JSON log files consisting of songs and artists data from the app all stored in S3.

The purpose of this project is to build an ETL pipeline that to extract data from the JSON files on S3 and store them in Redshift tables. The latter is to enhance scalability and efficiency of real-time quesries for the analytics team, by structuring the data on redshift into optimized fact and dimensional tables.

By the end of this project, the analytics team should be able to run real-time queries for insightful decision making on the app.
![S3-REDSHIFT](https://github.com/Tsakunelson/Data_Warehouse_Redshift-/blob/main/S3-vs-redshift%20(1).png)

# Data Sources

Two data sources in JSON format:

1. Song data: Log song features collected from the app

Each JSON file here contains metadata abbout a song and the artist of the song. For example, here is the filepath to a single song file of the dataset in S3:
''' song_data/A/B/C/TRABCEI128F424C983.json '''

Below is an example of what it contains:
''' {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0} '''

2. Log data: User activity data collected from user activity logs on the app

This dataset is JSON format as well, containing user generated event on the app, based on the songs above. Here is the filepath to a sample log file:
'''log_data/2018/11/2018-11-12-events.json'''


Below is a screenshot of the data table:
![Log Events Head](https://github.com/Tsakunelson/Data_Warehouse_Redshift-/blob/main/log-data.png)

# Files Description

The project package includes four files

create_tables.py: This files is in charge of creating the fact and dimensional tables on redshift

etl.py: Consists of extracting(E) data from S3 into temporary staging tables on reqshift, transforming(T) and Loading (L) the data from the staging tables into the fact and dimensional tables on Redshift.  

sql_queries.py: This is where all sql quesries are written (similar to a helper function containing the required sql statements)

dwh.cfg: As the name states, dwh.cfg = data warehouse house configurations; this it contains are the necessary configurations required to connect to your created data Redshift dataware house. Bellow are the parameters topopulate the dwh.cfg file for a succesfull connection:

'''
[CLUSTER]
HOST=''
DB_NAME=''
DB_USER=''
DB_PASSWORD=''
DB_PORT=5439

[IAM_ROLE]
ARN=''

[S3]
LOG_DATA=''
LOG_JSONPATH=''
SONG_DATA=''
'''

# Star DataBase Schema

Using the above song and events dataset, below is the star schema designed and optimized for real-time quering


    |Users|                     . |songs|                    
     ------ .                .     -----
               | songplays |
               .------------.
              .              .
             .                .
    |artists|                    .|time| 
     --------                     -----

## Fact Table
1. songplays - records in event data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Dimension Tables
2. users - users in the app
    - user_id, first_name, last_name, gender, level
3. songs - songs in music database
    - song_id, title, artist_id, year, duration
4. artists - artists in music database
    - artist_id, name, location, lattitude, longitude
5. time - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday


# Running the Project

1. Configure you dwh.cfg file to connect to Redshift

2. Create the Table Schemas
''' python create_tables,py '''

3. Run the etl process to create the optimized tables on Redshift
''' python etl.py '''


# Sample Use case
Query = ``` "SELECT DISTINCT u.user_id, u.first_name, u.last_name, u.gender, u.level FROM users u WHERE user_id IS NOT NULL;"```
