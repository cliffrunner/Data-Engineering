# Summary
the project allows users to go through the ETL process with postgresql. The data files are saved locally in the disk, which contain two categories: song data, and log data. Users are asked to load those data files, transform them and load them into a postgresql database following 3NF.

# How to run the code
  - 1. open a terminal in the same path as all other files
  - 2. type in 'python3 create_tables.py' in command line to create empty tables
  - 3. type in 'python3 etl.py' in command line to process data files and load data into corresponding tables

# File structure
The folder has the following files and folder:
  - data: Data folder contains all the data files needed to create the database.
  - sql_query.py: python file containing all sql queries to create tables and insert data into tables
  - create_tables.py: python file that creates tables.
  - etl.py: python file that load data files, transform data and inject data into their coresponding data tables.
  - et.ipynb: python notebook to test individual functions in elt.py
  - test.ipynb: python notebook to evaluate the correctness of final data tables.
  