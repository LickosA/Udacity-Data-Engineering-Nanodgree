# Data Pipelines with Airflow

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of CSV logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Data pipeline

The following data pipeline has been designed to meet the Sparkify data needs.

![alt text](airflow_project_dag.png)

## Redshift tables

The following tables are generated in Redshift using this pipeline:
- `fct_songplays` 
- `dim_artists`
- `dim_songs`
- `dim_users`
- `dim_time`

## Airflow operators

Several custom operators are used in the etl process:
- `stage_redshift.py` 
- `load_dimension.py`
- `load_fact.py` 
- `data_quality.py`