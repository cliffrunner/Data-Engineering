# DROP TABLES
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES
songplay_table_create = ("""
create table if not exists songplays (
    songplay_id serial primary key, 
    start_time date not null, 
    user_id integer not null references users, 
    level text, 
    song_id text references songs, 
    artist_id text references artists, 
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

# INSERT RECORDS

songplay_table_insert = ("""
        insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        values (%s, %s, %s, %s, %s, %s, %s, %s)
    """)

user_table_insert = ("""
        insert into users (user_id, first_name, last_name, gender, level)
        values (%s, %s, %s, %s, %s)
        on conflict (user_id)
        do update set level=excluded.level
    """)

song_table_insert = ("""
        insert into songs (song_id, title, artist_id, year, duration)
        values (%s, %s, %s, %s, %s)
        ON CONFLICT (song_id) 
        DO NOTHING;
        """)

artist_table_insert = ("""
        insert into artists (artist_id, name, location, latitude, longitude)
        values (%s, %s, %s, %s, %s)
        on conflict (artist_id)
        do nothing;
    """)


time_table_insert = ("""
        insert into time (start_time, hour, day, week, month, year, weekday)
        values (%s, %s, %s, %s, %s, %s, %s)
        on conflict (start_time)
        do nothing;
    """)

# FIND SONGS

song_select = ("""
        select songs.song_id, songs.artist_id
        from songs
        join artists
        on songs.artist_id = artists.artist_id
        where songs.title = %s and artists.name = %s and songs.duration = cast(%s as integer)
    """)
# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]