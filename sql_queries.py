# -*- coding: utf-8 -*-
"""SQL Queries used in the development.

The current script has all the statements to create TABLES and INSERT
data into them, also this includes DROP statements and other queries to
retrieve information and insights.

Mantaniner: Rolando Gonzalez
Version: 1.0.0
"""
# Standard library imports
# None

# Third-party imports
# None

# Propietary imports
# None

# DROP TABLES
songplay_table_drop = ("DROP TABLE songplay;\n")
user_table_drop = ("DROP TABLE users;\n")
song_table_drop = ("DROP TABLE songs;\n")
artist_table_drop = ("DROP TABLE artists;\n")
time_table_drop = ("DROP TABLE time;\n")

# CREATE TABLES

# Dimension Table 'time'
time_table_create = (
    "CREATE TABLE IF NOT EXISTS time\n"
    "(\n"
    "    start_time TIMESTAMP,\n"
    "    hour SMALLINT,\n"
    "    day SMALLINT,\n"
    "    week SMALLINT,\n"
    "    month SMALLINT,\n"
    "    year SMALLINT,\n"
    "    weekday SMALLINT\n"
    ");\n"
)

# Dimension Table 'users'
user_table_create = (
    "CREATE TABLE IF NOT EXISTS users\n"
    "(\n"
    "    user_id INT,\n"
    "    first_name VARCHAR(32),\n"
    "    last_name VARCHAR(32),\n"
    "    gender CHAR(1),\n"
    "    level VARCHAR(32)\n"
    ");\n"
)

# Dimension Table 'artists'
artist_table_create = (
    "CREATE TABLE IF NOT EXISTS artists\n"
    "(\n"
    "    artist_id INT,\n"
    "    name VARCHAR(32),\n"
    "    location VARCHAR(128),\n"
    "    latitude NUMERIC(7, 5),\n"
    "    longitude NUMERIC(8, 5)\n"
    ");\n"
)

# Dimension Table 'songs'
song_table_create = (
    "CREATE TABLE IF NOT EXISTS songs\n"
    "(\n"
    "    song_id INT,\n"
    "    title VARCHAR(128),\n"
    "    artist_id INT,\n"
    "    year SMALLINT,\n"
    "    duration NUMERIC(8, 5)\n"
    ");\n"
)

# Fact Table 'songplay'
songplay_table_create = (
    "CREATE TABLE IF NOT EXISTS songplay\n"
    "(\n"
    "    songplay_id INT,\n"
    "    start_time TIMESTAMP NOT NULL,\n"
    "    user_id INT,\n"
    "    level VARCHAR(32),\n"
    "    song_id INT,\n"
    "    artist_id INT,\n"
    "    session_id INT,\n"
    "    location VARCHAR(128),\n"
    "    user_agent TEXT\n"
    ");\n"
)

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]