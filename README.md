# Building a Data Warehouse for Sparkify

This project summarises efforts to extract raw songplay data and log data from a S3 bucket, transform and load those into staging tables in Redshift and convert them into a set of analytical tables: 1 fact table (songplays) and 4 dimension tables (songs, artists, users, and datetime). 

# Files

### sql_queries.py
Contains all of the necessary queries for creating tables and inserting data into them.

### create_tables.py
Python script that resets the database by droping any existing tables and then creating them again with no rows.

### etl.py
Main Python script for extracting raw data, transforming certain columns and loading them into Redsfhit tables.

### dwh.cfg
Contains all required settings to access S3 and Redshift in AWS.

# How to run

Clone the remote repository:
```
git clone https://github.com/tuliorc/dwh_sparkify.git
```

Go into your new local repository:
```
cd dwh_sparkify
```

Make sure you have Python3 installed in your computer:
```
python -V
```

In case you don't, install it:
```
sudo apt-get update
sudo apt-get install python3.6
```
Execute the script for creating tables in Redshift:
```
python3 create_tables.py
```
Then, execute the ETL script to load data into staging tables and insert data into analytics tables after transformation:
```
python3 etl.py
```
This will allow you to perform queries on the Redshift cluster to check if the analytics table return values as expected.