# Summary
In this project, we need to use Redshift for data ETL. The data ETL contains the following steps:
  - Step 1. create corresponding staging tables and final tables
  - Step 2. copy data from S3 bucket to Redshift staging tables
  - Step 3. create star schema and insert data from staging tables to final data tables.

## Step 1
in this step, we need to first drop all the pre-existing tables and then create empty staging tables and final tables.

## Step 2
All tables in the S3 buckets are organized with proper prefix. Therefore we can just use a simple copy script to copy data from S3 to Redshift. One example is below.
`copy staging_events` 
`from s3://udacity-dend/log_data`
`credentials 'aws_iam_role=iam role'` 
`json 'auto'`
In this example, we copy data from a S3 bucket with prefix `log_data` to the staging table `staging_events` in Redshift

## Step 3
This step is very similar to what we have worked on in Project 1. The main difference is that in Project 1, we insert into the final table by rows, but in this project we insert by table. 
The `on conflicing` statement in Project 1 is no longer available in this project. The following is a proper replacement of `on conflicting` using `distinct` statement.
`insert into songs (song_id, title, artist_id, year, duration)`
`    (`
`        select song_id, title, artist_id, year, duration`
`        from staging_songs`
`        where song_id not in (select distinct song_id from songs)`
`    )`
The above statement loop over the data table recursively to ensure the `song_id` is unique in the data table.