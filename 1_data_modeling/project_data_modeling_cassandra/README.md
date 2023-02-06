# Project: Data Modeling with Apache Cassandra

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.



## Project structure

Below are the steps implemented to carry out the project:
* ETL pipeline for preprocessing the files with the original data.
    + The `event_data` directory contains a series of csv files partitioned by date, which make up the raw dataset.
    + The preprocessed data is stored in the `event_datafile.csv` file.
* Data modeling.
    + Create appropiate Apache Cassandra tables to answer 3 specific questions.
    + Insert data from the preprocessed dataset to Cassandra tables.
    + Validate the data model by using 3 queries.

The `project_data_preprocessing_and_data_modeling.ipynb` notebook contains the steps to run both ETL and data modeling parts.  

Another way to develop the project is to run both parts separatedly. This approach is also provided:
1. ETL pipeline making use of Docker and a Python script (run `data_preprocessing.py`).
2. Data modeling and queries (`data_modeling.ipynb` notebook).

In both cases, we need to start a Cassandra server instance with Docker for the data modeling:

```bash
docker run \
    -v $(pwd)/data/cassandra_data:/var/lib/cassandra \
    -p 9042:9042 \
    --name cassandra_db \
    cassandra:4.1
```

