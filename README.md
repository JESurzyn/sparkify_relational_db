# Sparkify DB ReadMe
### Purpose
The purpose of this database is to provide a persistent datastore for analysts at the (imaginary) music streaming company Sparkify to perform dynamic song play analysis on.  Sparkify's new music streaming application collects data on songs and user activity and saves it to a directory in the form of JSON files.  As raw JSON files are not easily queriable, the analysts requested a database to allow for easy querying in order to explore business questions such as what songs users are listening to.


### Data Model
A relational database was chosen as the optimal format for this business need as the dataset is relatively small, the need for easy querying independent of schema is needed, and the data structure is relatively consistent.  A simple star schema was chosen with a table for songplay logs as the central fact table with a user table, an artist table, a time table, and a song table as dimension tables.  The songplay table is based on the log_data JSON files.  The user table and time table are also based on the log_data.  The artist table and song table are based on the song_data JSON files.


### ETL Overview
The ETL process was structured and written in the form of three python scripts.
**sql\_queries.py** contains all the SQL statements used by the ETL process.  These statements include dropping tables, creating tables, insert statements, etc.

**create\_tables.py** imports **sql\_queries.py** as a module and uses the query strings in psycopg2 python functions to execute said queries.  The script will drop the sparkify database if it exists.  The script will then create new tables (the tables stated in the **Data Model** section).

**etl.py** imports **sql\_queries.py** as a module.  The script then uses the insert queries from the module in psycopg2 functions to upsert the data from the JSON files to the tables created in **create\_tables.py** 


### Running the ETL
To run the actual ETL process, makes sure the filepaths in the **etl.py** script reference the correct path to song_data and log_data respectively.  Run **create\_tables.py** script first to create the database and the tables.  Run the **etl.py** script to populate the newly created database/tables.


### Additional
Also included in this repo are 2 Jupyter Notebooks: etl.ipynb and test.ipynb.  These were both used for unit testing various aspects of the table create and etl scripts.

Data quality checks can be performed by running the following queries to look for duplicate rows in the user, song, and artist tables:  
`SELECT user_id, COUNT(*) AS unique_user_count FROM users GROUP BY 1 HAVING COUNT(*) > 1;`  
`SELECT song_id, COUNT(*) AS unique_songid_count FROM songs GROUP BY 1 HAVING COUNT(*) > 1;`  
`SELECT artist_id, COUNT(*) AS unique_artistid_count FROM artists GROUP BY 1 HAVING COUNT(*) >1;`  