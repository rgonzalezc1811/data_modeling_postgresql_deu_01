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

    # convert timestamp column to datetime
    df["ts"] = pd.to_datetime(df["ts"], unit='ms')
    
    # insert time data records
    time_data = list(
        map(
            lambda item: [
                item,
                item.hour,
                item.day,
                item.week,
                item.month,
                item.year,
                item.weekday() + 1
            ],
            df["ts"].copy()
        )
    )
    column_labels = [
        'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'
    ]
    time_list_dict = [dict(zip(column_labels, row)) for row in time_data]
    time_df = pd.DataFrame(time_list_dict)
    time_df = time_df.drop_duplicates()
    time_df = time_df[column_labels]
    time_df["start_time"] = time_df["start_time"].dt.strftime('%Y-%m-%dT%H:%M:%S')

    for _, row in time_df.iterrows():
        cur.execute(sq.time_table_insert(time_df))

    # load user table
    columns = ["userId", "firstName", "lastName", "gender", "level"]
    user_df = df[columns].copy()
    user_df[:]["firstName"] = user_df["firstName"].map(
        single_quote_converter, na_action='ignore'
    )
    user_df[:]["lastName"] = user_df["lastName"].map(
        single_quote_converter, na_action='ignore'
    )
    user_df[:]["gender"] = user_df["gender"].str.upper()
    user_df[:]["userId"] = user_df["userId"].astype(str)
    user_df = user_df.drop_duplicates()
    user_df = user_df.drop_duplicates(subset='userId', keep='last')

    # insert user records
    for _, row in user_df.iterrows():
        cur.execute(sq.user_table_insert(user_df))

    # Prepare df
    columns = [
        "ts",
        "userId",
        "level",
        "song",
        "artist",
        "length",
        "sessionId",
        "location",
        "userAgent"
    ]
    df = df[columns]

    # Format timestamp tp be upload in the database
    df["ts"] = df["ts"].dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Apply filtering of single quotes
    df[:]["song"] = df["song"].map(
        single_quote_converter, na_action='ignore'
    )
    df[:]["artist"] = df["artist"].map(
        single_quote_converter, na_action='ignore'
    )
    df[:]["location"] = df["location"].map(
        single_quote_converter, na_action='ignore'
    )

    for _, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(
            sq.song_select(
                dataframe=pd.DataFrame([row[["song", "artist", "length"]]])
            )
        )
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = 'nan', 'nan'

        songplay_data = pd.DataFrame([row])
        # insert songplay record
        songplay_data = songplay_data.drop(columns=["length"])
        songplay_data["artist"] = artistid
        songplay_data["song"] = songid

        cur.execute(sq.songplay_table_insert(dataframe=songplay_data))

def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
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
        password="<password>",
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