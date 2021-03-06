# AWS Redshift Datawarhouse

## Table of contents
- [Introduction](#introduction)
- [Project Description](#project-description)
- [Project Datasets](#project-datasets)
  * [Song dataset](#song-dataset)
  * [Log dataset](#log-dataset)
- [Technologies](#technologies)
- [Setup](#setup)
- [Files](#files)
- [Tables](#tables)
  * [Staging Tables](#staging-tables)
  * [Dimensional Tables](#dimensional-tables)
  * [Distribution Style and Sort Key](#distribution-style-and-sort-key)
- [Analytics](#analytics)
  * [Top 3 played songs](#top-3-played-songs)
    + [Query](#query)
    + [Result](#result)
  * [Top 3 artists](#top-3-artists)
    + [Query](#query-1)
    + [Result](#result-1)
  * [Top 3 hours listening activity](#top-3-hours-listening-activity)
    + [Query](#query-2)
    + [Result](#result-2)


## Introduction
This project is one of Udacitys Data Engineering Nano Degree projects. It is requested in the 2nd course: Cloud Data Warhouses.

You are required to build a simple ETL pipeline that extracts data from AWS S3 buckets and insert it into AWS Redshift staging tables\, then transform the data into a set of facts/dimensions tables representing a star schema for analytical usage.

## Project Description
>A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

>As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Datasets
There are two datasets residing in S3:

- **Song data:** `s3://udacity-dend/song_data`
- **Log data:** `s3://udacity-dend/log_data`
- **Log data json path:** `s3://udacity-dend/log_json_path.json`

### Song dataset
- Subset of [Million Song Dataset](http://millionsongdataset.com/)
- JSON files
- Sample:

```{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}```

### Log dataset
- Generated by [Event Simulator](https://github.com/Interana/eventsim)
- JSON files
- Sample:

![Log dataset sample](assets/log-data.png "Log Dataset Sample")

## Technologies
- Python 3
- Jupyter Notebook
- AWS S3
- AWS Redshift

## Setup
- Create a Redshift cluster with an IAM role to read from S3 bucket
- Create a Security group and assign it to your cluster to be able to access your cluster from a remote environment
- Add the database and the cluster configurations in `dwh.cfg` file
- Run `python3 create_tables.py` to drop the staging and dimensional tables if they exist then recreate them
- Run `python3 etl.py` to fill up the tables
- Open `Analytics.ipynb` Jupyter and run the blocks

## Files
- `dwh.cfg`: Contains configuration parameters
- `sql_queries.py`: Contains table drop\, creation and insertion queries
- `create_tables.py`: Connects to AWS Redshift\, drop all tables if exist\, then execute creation queries
- `etl.py`: Connects to AWS Redshift and execute the insertion queries
- `Analytics.ipynb`: Connectes to AWS Redshift and execute some analytics queries

## Tables
### Staging Tables
- Staging tables are loaded from the given Udacity AWS S3 Buckets
- We have 2 staging tables:
 - `staging_events`: Contains loaded data from the Log dataset
 - `staging_songs`: Contains loaded data from the Song dataset

### Dimensional Tables
- Dimensional tables are loaded from staging tables
- We have 1 fact table and 4 dimension tables:
 - `songplays`: Fact table - Contains all logged events related to playing a song
 - `users`: Dimension table - Contains records of all users
 - `songs`: Dimension table - Contains records of all songs
 - `artists`: Dimension table - Contains records of all artists
 - `time`: Dimension table - Contains start time for songs played 
 
### Distribution Style and Sort Key
- Since all the **dimension tables** are small\, then I gave them a **distribution style ALL** and a **sort key on the ID** column
- For the **fact table**\, it should be the largest table when the data grows more\, then I gave it a **distribution style KEY** on song_id\, since **songs table** is expected to be the 2nd largest table.  
  I also gave it a **sort key** on start_time because it\'s a **datetime column**  
 
## Analytics
### Top 3 played songs
#### Query
```
SELECT t2.title, count(*) 
FROM songplays AS t1 
JOIN songs AS t2 ON t1.song_id = t2.song_id 
GROUP BY t2.title 
ORDER BY count(*) DESC 
LIMIT 3;
```
#### Result
| Title                                                | Count |
|------------------------------------------------------|-------|
| You're The One                                       | 37    |
| I CAN'T GET STARTED                                  | 9     |
| Catch You Baby (Steve Pitron & Max Sanna Radio Edit) | 9     |

### Top 3 artists
#### Query
```
SELECT t2.name, count(*) 
FROM songplays AS t1 
JOIN artists AS t2 ON t1.artist_id = t2.artist_id 
GROUP BY t2.name 
ORDER BY count(*) DESC 
LIMIT 3;
```
#### Result
| Name          | Count |
|---------------|-------|
| Dwight Yoakam | 37    |
| Kid Cudi      | 10    |
| Lonnie Gordon | 9     |

### Top 3 hours listening activity
#### Query
```
SELECT t2.hour, count(*) 
FROM songplays AS t1 
JOIN time AS t2 ON t1.start_time = t2.start_time 
GROUP BY t2.hour 
ORDER BY count(*) DESC 
LIMIT 3;
```
#### Result
| Hour | Count |
|------|-------|
| 17   | 40    |
| 18   | 26    |
| 15   | 25    |
