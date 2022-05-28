# Project 4: Data Lake
-------------------------

### Introduction

The purpose of this project is to build an ETL pipeline that will be able to extract song and log data from an S3 bucket, process the data using Spark and load the data back into s3 as a set of dimensional tables in spark parquet files. 

### Datasets
The datasets used are retrieved from the s3 bucket and are in the JSON format. There are two datasets namely log_data and song_data. The song_data dataset is a subset of the the Million Song Dataset while the log_data contains generated log files based on the songs in song_data.

### Database Schema
Using the song and log datasets, we need to create a star schema optimized for queries on song play analysis. This includes the following tables.
#### Fact Table 
+ **songplays** - records in event data associated with song plays i.e. records with page `NextSong`

#### Dimension Tables
+ **users** - users in the app
+ **songs** - songs in music database
+ **artists** - artists in music database
+ **time** - timestamps of records in **songplays** broken down into specific units

#### Data model
The song play data model is as follows :
![](Song_Data_Model.png)

### Project Structure
+ `etl.py` - This script reads data from S3, processes that data using Spark, and writes them back to S3.
+ `dl.cfg` - contains our AWS credentials.
+ `README.md` - provides details about this project and our ETL pipeline.


### How to Run
Execute ETL process by running `python etl.py`.