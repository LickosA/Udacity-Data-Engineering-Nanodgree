# Data Modeling with Postgres

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis.

## Project Description
In this Project, fact and dimension tables will be designed for a star schema for this particular purpose. An ETL pipeline will also be designed to transfer data from files in two local directories into these tables in Postgres using Python and SQL.

**The purpose of this project is to create a Postgres database and ETL pipeline to optimize queries to help Sparkify's analytics team.**


## Getting started

```bash 
python create_tables.py
python etl.py
```

## Python scripts

### File Structure

- `sql_queries.py` - It contains all the sql scripts for drop and create tables for storing song and log information.
- `create_tables.py` - drops and creates tables in postgres database.
- `etl.ipynb` - reads and processes the single file from song_data and log_data and loads the data into the postgres database.
- `etl.py` - reads and processes all the files from song_data and log_data and loads the data into the postgres database.
- `test.ipynb` - displays the rows of each table to validate the data.

### ETL pipeline

  ETL.py will work as following for processing the data:
- Connect to the sparkify datase. And it will drop and create all the tables.
- Parse out each json file and load all of the files into dataframe.
- Song_data and Log_data will be loaded into the fact and dimension tables.

## Tables

### Fact table
- songplays - records in log data associated with song plays
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension tables
- users - users in the app
user_id, first_name, last_name, gender, level
- songs - songs in music database
song_id, title, artist_id, year, duration
- artists - artists in music database
artist_id, name, location, latitude, longitude
- time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday