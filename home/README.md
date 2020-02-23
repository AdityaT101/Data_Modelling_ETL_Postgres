The Project shows a classic ETL procedure using Postgres.

The million song data-set is freely available and is used here to create fact tables and dimension table.

There are 2 files  - 'song_data' and 'log_data' which hold the data in json format.

The 'create_tables.py' connects to the database and defines functions to drop and create tables.

It runs the queries imported from 'sql_queries.py'. This script contains the create and insert queries for all the the tables.

The 'etl.py' does the actual etl work. It extracts the data from those 2 files - ('song_data' and 'log_data') and inserts them into respective tables.

The order of running th scripts- 
 1. sql_queries.py
 2. create_tables.py
 3. etl.py
 
then, use the test.ipynb to run select statements to see whether the data gets inserted into those respective tables.

