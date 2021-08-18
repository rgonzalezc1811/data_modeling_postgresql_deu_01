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

def process_song_file(cur, filepath, conn=None):
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
    artist_data["artist_name"] = artist_data["artist_name"].map(
        single_quote_converter, na_action='ignore'
    )
    artist_data["artist_location"] = artist_data["artist_location"].map(
        single_quote_converter, na_action='ignore'
    )
    cur.execute(sq.artist_table_insert(dataframe=artist_data))

    # insert song record
    columns = ["song_id", "title", "artist_id", "year", "duration"]
    song_data = df[columns].copy()
    song_data["title"] = song_data["title"].map(
            single_quote_converter, na_action='ignore'
    )
    cur.execute(sq.song_table_insert(dataframe=song_data))

def process_log_file(cur, filepath, conn=None):
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
                item.strftime('%Y-%m-%dT%H:%M:%S'),
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
    time_df = time_df.reset_index(drop=True)
    time_df = time_df[column_labels]

    # Insert time records by batch queries.
    start_index = 0
    end_index = time_df.shape[0]
    batch_size = 5_000
    for index in range(start_index, end_index, batch_size):
        print(f"\nSend 'time' records batch from idx '{index}'...")
        query = sq.time_table_insert(
            dataframe=time_df.iloc[index:index + batch_size]
        )
        cur.execute(query)
        conn.commit()

    # load user table
    columns = ["userId", "firstName", "lastName", "gender", "level"]
    user_df = df[columns].copy()
    user_df["firstName"] = user_df["firstName"].map(
        single_quote_converter, na_action='ignore'
    )
    user_df["lastName"] = user_df["lastName"].map(
        single_quote_converter, na_action='ignore'
    )
    user_df["gender"] = user_df["gender"].str.upper()
    user_df["userId"] = user_df["userId"].astype(str)
    user_df = user_df.drop_duplicates()
    user_df = user_df.drop_duplicates(subset='userId', keep='last')
    user_df = user_df.reset_index(drop=True)

    # Insert user records by batch queries.
    start_index = 0
    end_index = user_df.shape[0]
    batch_size = 5_000
    for index in range(start_index, end_index, batch_size):
        print(f"Send 'users' records batch from idx '{index}'...")
        query = sq.user_table_insert(
            dataframe=user_df.iloc[index:index + batch_size]
        )
        cur.execute(query)
        conn.commit()

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
    df = df[columns].copy()

    # Format timestamp tp be upload in the database
    df["ts"] = df["ts"].dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Apply filtering of single quotes
    df["song"] = df["song"].map(
        single_quote_converter, na_action='ignore'
    )
    df["artist"] = df["artist"].map(
        single_quote_converter, na_action='ignore'
    )
    df["location"] = df["location"].map(
        single_quote_converter, na_action='ignore'
    )
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    df = df.reset_index(level=0)

    # Insert song_id and artist_id into the dataframe
    start_index = 0
    end_index = df.shape[0]
    batch_size = 5_000
    columns_in = ["index", "song", "artist", "length"]
    columns_out = ["song", "artist"]
    for index in range(start_index, end_index, batch_size):
        print(f"Get 'song_id' and 'artist_id' on batch from idx '{index}'...")
        query = sq.song_select(
            dataframe=df[columns_in].iloc[index:index + batch_size]
        )
        temp_df = pd.read_sql(query, conn)
        df.loc[index:index + temp_df.shape[0], columns_out] = temp_df

    df[columns_out] = df[columns_out].astype(str)
    df[columns_out[0]] = df[columns_out[0]].map(
        lambda value: value if value != 'None' else 'nan'
    )
    df[columns_out[1]] = df[columns_out[1]].map(
        lambda value: value if value != 'None' else 'nan'
    )
    df = df.drop(columns=['index', 'length'])

    # Insert songplays records by batch queries
    start_index = 0
    end_index = df.shape[0]
    batch_size = 5_000
    for index in range(start_index, end_index, batch_size):
        print(f"Send 'songplays' records batch from idx '{index}'...")
        query = sq.songplay_table_insert(
            dataframe=df.iloc[index:index + batch_size]
        )
        cur.execute(query)
        conn.commit()

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
        func(cur, datafile, conn)
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
    query = (
        "SELECT *\n"
        "FROM songplay s\n"
        "WHERE  s.song_id IS NOT NULL AND s.artist_id IS NOT NULL;\n"
    )

    # I spent a lot of time just to run on Udacity's workspace instead locally >:)
    print("\n\nHow many rows have song_id and artist_id in table 'songplay'?\n")
    print(f"Use the following SQL statement:\n{query}\n")
    songplay_with_ids = pd.read_sql(query, conn)
    length = songplay_with_ids.shape[0]
    print(f"There are {length} rows with this data in 'songplay' table :c\n")
    print(songplay_with_ids)
    # Well, now runs faster than I expected XD

    conn.close()
    sys.exit()

if __name__ == "__main__":
    main()