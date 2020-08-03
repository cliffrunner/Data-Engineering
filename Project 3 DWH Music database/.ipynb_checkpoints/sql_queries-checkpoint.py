import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_events (
    artist text,
    auth text,
    first_name text,
    gender char(2),
    item_in_session smallint,
    last_name text,
    length real,
    level varchar(10),
    location text,
    method varchar(3),
    page text,
    registration bigint,
    session_id smallint,
    song text,
    status smallint,
    ts bigint,
    user_agent text,
    user_id smallint
)
""")

staging_songs_table_create = ("""
create table if not exists staging_songs (
    num_songs smallint,
    artist_id text,
    artist_latitude real,
    artist_longitude real,
    artist_location text,
    artist_name text,
    song_id text,
    title text,
    duration real,
    year smallint
)
""")

songplay_table_create = ("""
create table if not exists songplays (
    songplay_id bigint identity(0,1) primary key, 
    start_time bigint, 
    user_id smallint, 
    level text, 
    song_id text, 
    artist_id text, 
    session_id integer, 
    location text,
    user_agent text
)
""")

user_table_create = ("""
create table if not exists users (
    user_id integer primary key, 
    first_name text, 
    last_name text, 
    gender text, 
    level text
)
""")

song_table_create = ("""
create table if not exists songs (
    song_id text primary key, 
    title text, 
    artist_id text, 
    year integer, 
    duration integer
)
""")

artist_table_create = ("""
create table if not exists artists (
    artist_id text primary key, 
    name text, 
    location text, 
    latitude numeric, 
    longitude numeric
)
""")

time_table_create = ("""
create table if not exists time (
    start_time text primary key, 
    hour integer, 
    day integer, 
    week integer, 
    month integer, 
    year integer, 
    weekday integer
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events 
from {}
credentials 'aws_iam_role={}' 
json 'auto'
""".format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN']))

staging_songs_copy = ("""
copy staging_songs
from {}
credentials 'aws_iam_role={}' 
json 'auto'
""".format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN']))

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    (
        select event.ts, event.user_id, event.level, song.song_id, song.artist_id, event.session_id, event.location, event.user_agent
        from staging_events event
        join staging_songs song
        on event.artist=song.artist_name
        and event.song=song.title
        and event.length=song.duration
        where page='NextSong'
    )
""")

user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    (
        select user_id, first_name, last_name, gender, level
        from staging_events
        where user_id not in (select distinct user_id from users)
        and user_id is not null
        and page = 'NextSong'
    )
""")

song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration)
    (
        select song_id, title, artist_id, year, duration
        from staging_songs
        where song_id not in (select distinct song_id from songs)
    )
""")

artist_table_insert = ("""
    insert into artists (artist_id, name, location, latitude, longitude)
    (
        select artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        from staging_songs
        where artist_id not in (select distinct artist_id from artists)
    )
""")

time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday)
    (
        select start_time, 
            extract(hour from start_time) as hour, 
            extract(day from start_time) as day,
            extract(week from start_time) as week,
            extract(month from start_time) as month,
            extract(year from start_time) as year,
            extract(DOW from start_time) as weekday
        from (select distinct TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time from staging_events)
    )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
