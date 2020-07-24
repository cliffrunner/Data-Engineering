import os
import glob
import psycopg2
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    load song files from disk, and insert items to the songs and artists table
    """
    df = pd.read_json(filepath, lines=True)
    song_data = tuple(df.loc[0,['song_id', 'title', 'artist_id', 'year', 'duration']])
    cur.execute(song_table_insert, song_data)
    
    artist_data = tuple(df.loc[0,['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    load log files from disk, and insert items to the time, users, and songplays tables
    """

    df = pd.read_json(filepath, lines=True)

    df = df[df.page=='NextSong']

    df.ts = pd.to_datetime(df.ts)
    
    time_data = pd.DataFrame(df.ts)
    time_data['hour'] = time_data.ts.dt.hour
    time_data['day'] = time_data.ts.dt.day
    time_data['week'] = time_data.ts.dt.week
    time_data['month'] = time_data.ts.dt.month
    time_data['year'] = time_data.ts.dt.year
    time_data['weekday'] = time_data.ts.dt.weekday
    time_data.ts = time_data.ts.astype(str)
    time_df = time_data[['ts','hour','day','week','month','year','weekday']]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, int(row.length)))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = [str(row.ts), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    loop over all files recursively given the path,
    and process files with the 'func' input
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    this file orchestrates the ETL process:
    extract data files from disk, transform data and load data into corresponding tables
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()