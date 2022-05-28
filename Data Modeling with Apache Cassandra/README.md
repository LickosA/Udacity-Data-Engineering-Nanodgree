# Data Modeling with Apache Cassandra

## Purpose
The purpose of this project is to create an Apache Cassandra database for a ficticious startup called Sparkify to query on song play data. The raw data is in a directory of CSV files, and we will build a ETL pipeline to transform the raw data into the Apache Cassandra database.

## Dataset
There is one dataset called event_data which is in a directory of CSV files partitioned by date.

## Queries
For NoSQL databases, we design the schema based on the queries we know we want to perform. For this project, we have three queries:

- Find artist, song title and song length that was heard during sessionId=338, and itemInSession=4.
SELECT artist, song, length from table_1 WHERE sessionId=338 AND itemInSession=4
- Find name of artist, song (sorted by itemInSession) and user (first and last name) for userid=10, sessionId=182.
SELECT artist, song, firstName, lastName FROM table_2 WHERE userId=10 and sessionId=182
- Find every user name (first and last) who listened to the song 'All Hands Against His Own'.
SELECT firstName, lastName WHERE song='All Hands Againgst His Own'

    
## Project Structure
- **event_data** folder nested at the home of the project, where all needed data reside.
- **Project_1B_ Project_Template.ipynb** the code itself.
- **event_datafile_new.csv** a smaller event data csv file that will be used to insert data into the Apache Cassandra tables.
- **images** a screenshot of what the denormalized data should appear like in the event_datafile_new.csv.
- **README.md** current file, provides description of the project.
    
# Build Instructions

Run each portion of **Project_1B_Project_Template.ipynb**.

