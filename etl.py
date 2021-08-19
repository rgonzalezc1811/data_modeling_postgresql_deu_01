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
import sql_queries

def single_quote_converter(sentence):
    """Handle single-quote character, insert a tag identifier instead.

    From a given string, change all the single-quote characters (') to
    the following tag:

        <single_quote_tag>

    The SQL statements written in Python and loaded by Pandas usually
    adds double-quote in attributes of type string if a single-quote
    appears in a row, the problem of including a double-quote in a SQL
    statement is that a word between double quotes are interpreted as
    a column instead a value.

    To avoid this issue, the single quotes are substituted by the
    single-quote tag.

    When inserting the tagged word into a SQL generator like
    'sql_queries.Queries()song_table_insert(dataframe=df, verbose=True)'
    the build-in method use a regex to replace the tag to the required
    double single-quote that can be accepted in a SQL INSERT statement.

    Parameters
    ----------
    setence : String
        description -> A sentence to process to serach single-quotes,
            if a single-quote is found, it is substituted by the
            single-quote tag '<single_quote_tag>'
        format -> No apply
        options -> No apply

    Returns
    -------
    sentence : String
        description -> Sentence filtered of single-quote characters
        format -> No apply
        options -> No apply
    """
    sentence = re.sub(r'\'', r'<single_quote_tag>', sentence)

    return sentence

def process_song_file(cur, filepath, conn=None):
    """For each song JSON file, process and INSERT records.

    * Initialize the Queries() instance.
    * Read the song JSON file and store data in a dataframe.
    * Define the attributes to use for artist records.
    * Filter string attributes of single-quotes.
    * Create the query from the dataframe.
    * Execute query to insert the artist records into table 'artists'.
    * Define the attributes to use for songs records.
    * Filter string attributes of single-quotes.
    * Create the query from the dataframe.
    * Execute query to insert the song records into table 'songs'.

    Parameters
    ----------
    cur : PostgreSQL Cursor Instance
        description -> The cursor required to execute build-in queries.
        format -> No apply
        options -> No apply

    filepath : String
        description -> The JSON file location as a full path.
        format -> '<root_path>/data_modeling_postgresql_deu_01/data/
                   song_data/A/<AB>/<ABC>/<json_file>.json'
        options -> No apply

    conn : PostgreSQL Connection Instance
        description -> The connection active to the 'sparkify' database
        format -> No apply
        options -> 'None': On this method, the connection is not
            required.
    """
    del conn
    sql = sql_queries.Queries()
    # open song file
    songs_df = pd.read_json(filepath, lines=True)

    # insert artist record
    columns = [
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    ]
    artist_data = songs_df[columns].copy()
    artist_data["artist_name"] = artist_data["artist_name"].map(
        single_quote_converter, na_action='ignore'
    )
    artist_data["artist_location"] = artist_data["artist_location"].map(
        single_quote_converter, na_action='ignore'
    )
    query = sql.artist_table_insert(dataframe=artist_data)
    cur.execute(query)

    # insert song record
    columns = ["song_id", "title", "artist_id", "year", "duration"]
    song_data = songs_df[columns].copy()
    song_data["title"] = song_data["title"].map(
            single_quote_converter, na_action='ignore'
    )
    query = sql.song_table_insert(dataframe=song_data)
    cur.execute(query)

def process_log_file(cur, filepath, conn=None):
    """For each log JSON file, process and INSERT records.

    * Initialize the Queries() instance.
    * Read the log JSON file and store data in a dataframe.
    * Convert column of datetime into human-readble format.
    * Create a list of values with data generated from the datetime.
    * Define attributes names.
    * Generate a dataframe from list and dictionaries.
    * Drop duplicates.
    * Reorder attributes in the dataframe, order is important, the SQL
      query generator required it.
    * Insert time records by batches to the table 'time':
        * Each batch sends up to 5,000 records.
        * Create the batch query SQL statement from the dataframe.
        * Execute the query and commit to the database.
    * Define the attributes to use for user records.
    * Create the users dataframe
    * Filter string attributes of single-quotes.
    * Insert users records by batches to the table 'users':
        * Each batch sends up to 5,000 records.
        * Create the batch query SQL statement from the dataframe.
        * Execute the query and commit to the database.
    * Define the attributes to use for songplay records.
    * Generate the songplays dataframe.
    * Convert attribute of datetime into human-readble format.
    * Filter string attributes of single-quotes.
    * Create index attribute to control the order of records.
    * Use a SQL statement to change song title and artist name, using
      additionally the song duration and index to retrieve the
      corresponding song id and artist id.
    * Insert the results in the dataframe, if the song and artist is
      missed, insert a null value instead.
    * Drop the columns related to song duration and index order.
    * Insert songplays records by batches to the table 'songplays':
        * Each batch sends up to 5,000 records.
        * Create the batch query SQL statement from the dataframe.
        * Execute the query and commit to the database.

    Parameters
    ----------
    cur : PostgreSQL Cursor Instance
        description -> The cursor required to execute build-in queries.
        format -> No apply
        options -> No apply

    filepath : String
        description -> The JSON file location as a full path.
        format -> '<root_path>/data_modeling_postgresql_deu_01/data/
                   song_data/A/<AB>/<ABC>/<json_file_name>.json'
        options -> No apply

    conn : PostgreSQL Connection Instance
        description -> The connection active to the 'sparkifydb' database
        format -> No apply
        options -> On this method, the connection is required, None
            value is not allowed.
    """
    sql = sql_queries.Queries()
    # open log file
    logs_df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    logs_df = logs_df[logs_df["page"] == 'NextSong']

    # convert timestamp column to datetime
    logs_df["ts"] = pd.to_datetime(logs_df["ts"], unit='ms')

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
            logs_df["ts"].copy()
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
        query = sql.time_table_insert(
            dataframe=time_df.iloc[index:index + batch_size]
        )
        cur.execute(query)
        conn.commit()

    # load user table
    columns = ["userId", "firstName", "lastName", "gender", "level"]
    user_df = logs_df[columns].copy()
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
        query = sql.user_table_insert(
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
    songplays_df = logs_df[columns].copy()
    del logs_df

    # Format timestamp tp be upload in the database
    songplays_df["ts"] = songplays_df["ts"].dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Apply filtering of single quotes
    songplays_df["song"] = songplays_df["song"].map(
        single_quote_converter, na_action='ignore'
    )
    songplays_df["artist"] = songplays_df["artist"].map(
        single_quote_converter, na_action='ignore'
    )
    songplays_df["location"] = songplays_df["location"].map(
        single_quote_converter, na_action='ignore'
    )
    songplays_df = songplays_df.drop_duplicates()
    songplays_df = songplays_df.reset_index(drop=True)
    songplays_df = songplays_df.reset_index(level=0)

    # Insert song_id and artist_id into the dataframe
    start_index = 0
    end_index = songplays_df.shape[0]
    batch_size = 5_000
    columns_in = ["index", "song", "artist", "length"]
    columns_out = ["song", "artist"]
    for index in range(start_index, end_index, batch_size):
        print(f"Get 'song_id' and 'artist_id' on batch from idx '{index}'...")
        query = sql.song_select(
            dataframe=songplays_df[columns_in].iloc[index:index + batch_size]
        )
        temp_df = pd.read_sql(query, conn)
        songplays_df.loc[index:index + temp_df.shape[0], columns_out] = temp_df

    songplays_df[columns_out] = songplays_df[columns_out].astype(str)
    songplays_df[columns_out[0]] = songplays_df[columns_out[0]].map(
        lambda value: value if value != 'None' else 'nan'
    )
    songplays_df[columns_out[1]] = songplays_df[columns_out[1]].map(
        lambda value: value if value != 'None' else 'nan'
    )
    songplays_df = songplays_df.drop(columns=['index', 'length'])

    # Insert songplays records by batch queries
    start_index = 0
    end_index = songplays_df.shape[0]
    batch_size = 5_000
    for index in range(start_index, end_index, batch_size):
        print(f"Send 'songplays' records batch from idx '{index}'...")
        query = sql.songplay_table_insert(
            dataframe=songplays_df.iloc[index:index + batch_size]
        )
        cur.execute(query)
        conn.commit()

def process_data(cur, conn, filepath, func):
    """Read datasts directory an execute process to insert records.

    * Get the list of JSON files in the given 'filepath'.
    * Shows the total number of files found.
    * Iterate each file name.
        * Use the function given in 'func' and the current file path to
          process JSON data and execute INSERT statements.
        * Commit the SQL statements to INSERY records into Sparkify's
          database

    Parameters
    ----------
    cur : PostgreSQL Cursor Instance
        description -> The cursor required to execute build-in queries.
        format -> No apply
        options -> No apply

    conn : PostgreSQL Connection Instance
    description -> The connection active to the 'sparkifydb' database
    format -> No apply
    options -> No apply

    filepath : String
        description -> The path to specific data
        format -> No apply
        options -> {
            'data/song_data': To get song JSON file names,
            ''data/log_data': To get logs JSON file names
        }

    func : Functiom
        description -> The required function to process specific JSON
            file
        format -> No apply
        options -> {
            process_song_file: Use it to process the song JSON files,
            process_log_file: Use it to process the logs JSON files
        }
    """
    # get all files matching extension from directory
    all_files = []
    for root, _, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for file_name in files :
            all_files.append(os.path.abspath(file_name))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile, conn)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

def main():
    """Main to execute complete ETL pipeline.

    * Open connection to the Sparkify's database.
    * Create the SQL cursor instance.
    * Process and insert song JSON files.
    * Process and insert logs JSON files.
    * Create a query to retrieve records with complete data from the
      'songplays' table.
    * Execute and commit the query to print the total number of records
      with complete data.
    * Print the loaded data.
    * Close connection.
    """
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
