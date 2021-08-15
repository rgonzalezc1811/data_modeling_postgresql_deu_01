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
    "    start_time timestamp PRIMARY KEY,\n"
    "    hour smallint NOT NULL,\n"
    "    day smallint NOT NULL,\n"
    "    week smallint NOT NULL,\n"
    "    month smallint NOT NULL,\n"
    "    year smallint NOT NULL,\n"
    "    weekday smallint NOT NULL,\n"
    "    CHECK (hour >= 0 AND hour <= 23),\n"
    "    CHECK (day >= 1 AND day <= 31),\n"
    "    CHECK (week >= 1 AND week <= 53),\n"
    "    CHECK (month >= 1 AND month <= 12),\n"
    "    CHECK (year > 1900 AND year <= 2999),\n"
    "    CHECK (weekday >= 1 AND weekday <= 8)\n"
    ");\n"
)

# Dimension Table 'users'
user_table_create = (
    "CREATE TABLE IF NOT EXISTS users\n"
    "(\n"
    "    user_id int PRIMARY KEY,\n"
    "    first_name varchar(32),\n"
    "    last_name varchar(32),\n"
    "    gender CHAR(1),\n"
    "    level varchar(32),\n"
    "    CHECK (user_id >= 0)\n"
    ");\n"
)

# Dimension Table 'artists'
artist_table_create = (
    "CREATE TABLE IF NOT EXISTS artists\n"
    "(\n"
    "    artist_id int PRIMARY KEY,\n"
    "    name varchar(32),\n"
    "    location varchar(128),\n"
    "    latitude numeric(7, 5),\n"
    "    longitude numeric(8, 5),\n"
    "    CHECK (latitude >= -90 AND latitude <= 90),\n"
    "    CHECK (longitude >= -180 AND artist_id <= 180)\n"
    ");\n"
)

# Dimension Table 'songs'
song_table_create = (
    "CREATE TABLE IF NOT EXISTS songs\n"
    "(\n"
    "    song_id int,\n"
    "    title varchar(128),\n"
    "    artist_id int,\n"
    "    year smallint,\n"
    "    duration numeric(8, 5)\n"
    ");\n"
)

# Fact Table 'songplay'
songplay_table_create = (
    "CREATE TABLE IF NOT EXISTS songplay\n"
    "(\n"
    "    songplay_id int,\n"
    "    start_time timestamp NOT NULL,\n"
    "    user_id int,\n"
    "    level varchar(32),\n"
    "    song_id int,\n"
    "    artist_id int,\n"
    "    session_id int,\n"
    "    location varchar(128),\n"
    "    user_agent text\n"
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