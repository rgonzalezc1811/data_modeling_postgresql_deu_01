# -*- coding: utf-8 -*-
"""Script to perform ETL process.

Mantaniner: Rolando Gonzalez
Version: 1.0.0
"""
# Standard library imports
import glob
import os
import re
import sys

# Third-party imports
import psycopg2
import pandas as pd

# Propietary imports
import sql_queries as sq

def single_quote_converter(sentence):
    sentence = re.sub(r'\'', r'<single_quote_tag>', sentence)

    return sentence

def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert artist record
    columns = [
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    ]
    artist_data = df[columns].copy()
    artist_data[:]["artist_name"] = artist_data["artist_name"].map(
        single_quote_converter, na_action='ignore'
    )
    artist_data[:]["artist_location"] = artist_data["artist_location"].map(
        single_quote_converter, na_action='ignore'
    )
    cur.execute(sq.artist_table_insert(dataframe=artist_data))

    # insert song record
    columns = ["song_id", "title", "artist_id", "year", "duration"]
    song_data = df[columns]
    song_data[:]["title"] = song_data["title"].map(
            single_quote_converter, na_action='ignore'
    )
    cur.execute(sq.song_table_insert(dataframe=song_data))

def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == 'NextSong']

    print(df)
    print(df.shape)
    return

    # convert timestamp column to datetime
    t = None
    
    # insert time data records
    time_data = None
    column_labels = None
    time_df = None

    for i, row in time_df.iterrows():
        cur.execute(sq.time_table_insert, list(row))

    # load user table
    user_df = None

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(sq.user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(sq.song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = None
        cur.execute(sq.songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    # Get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

def main():
    conn = psycopg2.connect(
        user="admin",
        password="98573Hgte", # <password>
        host="localhost",
        port="5432",
        dbname="sparkifydb"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()
    sys.exit()

if __name__ == "__main__":
    main()