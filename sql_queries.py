# -*- coding: utf-8 -*-
"""SQL Queries used in the development.

The current script has all the statements to create TABLES and INSERT
data into them, also this includes DROP statements and other queries to
retrieve information and insights.

Mantaniner: Rolando Gonzalez
Version: 1.0.0
"""
# Standard library imports
import re

# Third-party imports
# None

# Propietary imports
# None

class Queries():
    """Queries class.

    SQL statements to use for creation, drop, retrieving, manipulating
    and storing data; the queries are designed to be use only with the
    PostgreSQL.

    Attributes
    ----------
    create_table_queries : String List
        description -> Each element has a query to create an especific
            table, the creation order is:
                1. 'artist_table_create'
                2. 'song_table_create'
                3. 'time_table_create'
                4. 'user_table_create'
                5. 'songplay_table_create'

            Create tables in this order allows to reference the primary
            keys from tables with an especific foreign key.
        format -> No apply
        options -> No apply

    drop_table_queries : String List
        description -> Each element has a query to drop an especific
            table, the deletion order is:
                1. 'songplay_table_drop'
                2. 'time_table_drop'
                3. 'user_table_drop'
                4. 'song_table_drop'
                5. 'artist_table_drop'

            The tables are created with especific restrictions, so this
            deletion order is critical.
        format -> No apply
        options -> No apply

    Methods
    -------
    artist_table_insert : Static Method
        Method to create an INSERT query to the table 'artists' from a
        dataframe.

    song_table_insert : Static Method
        Method to create an INSERT query to the table 'songs' from a
        dataframe.

    time_table_insert : Static Method
        Method to create an INSERT query to the table 'time' from a
        dataframe.

    user_table_insert : Static Method
        Method to create an INSERT query to the table 'users' from a
        dataframe.

    songplay_table_insert : Static Method
        Method to create an INSERT query to the table 'songplays' from a
        dataframe.

    song_select : Static Method
        Method to create a retrieve query statement from a dataframe
        with artists name, title songs and duretion of the song. The
        tables used are 'songs' and 'artists'.
    """
    def __init__(self):
        # CREATE TABLES
        # Dimension Table 'time'
        time_table_create = (
            "CREATE TABLE IF NOT EXISTS \"time\"\n"
            "(\n"
            "    start_time timestamp PRIMARY KEY,\n"
            "    hour smallint NOT NULL,\n"
            "    day smallint NOT NULL,\n"
            "    week smallint NOT NULL,\n"
            "    month smallint NOT NULL,\n"
            "    \"year\" smallint NOT NULL,\n"
            "    weekday smallint NOT NULL,\n"
            "    CHECK (hour >= 0 AND hour <= 23),\n"
            "    CHECK (day >= 1 AND day <= 31),\n"
            "    CHECK (week >= 1 AND week <= 53),\n"
            "    CHECK (month >= 1 AND month <= 12),\n"
            "    CHECK (\"year\" > 1900 AND \"year\" <= 2999),\n"
            "    CHECK (weekday >= 1 AND weekday <= 8)\n"
            ");\n"
        )

        # Dimension Table 'users'
        user_table_create = (
            "CREATE TABLE IF NOT EXISTS users\n"
            "(\n"
            "    user_id int PRIMARY KEY,\n"
            "    first_name varchar(256),\n"
            "    last_name varchar(256),\n"
            "    gender CHAR(1),\n"
            "    level varchar(32),\n"
            "    CHECK (user_id >= 0),\n"
            "    CHECK (gender = 'F' OR gender = 'M')\n"
            ");\n"
        )

        # Dimension Table 'artists'
        artist_table_create = (
            "CREATE TABLE IF NOT EXISTS artists\n"
            "(\n"
            "    artist_id char(18) PRIMARY KEY,\n"
            "    \"name\" varchar(256),\n"
            "    location varchar(512),\n"
            "    latitude numeric(7, 5),\n"
            "    longitude numeric(8, 5),\n"
            "    CHECK (latitude >= -90 AND latitude <= 90),\n"
            "    CHECK (longitude >= -180 AND longitude <= 180)\n"
            ");\n"
        )

        # Dimension Table 'songs'
        song_table_create = (
            "CREATE TABLE IF NOT EXISTS songs\n"
            "(\n"
            "    song_id char(18) PRIMARY KEY,\n"
            "    title varchar(256),\n"
            "    artist_id char(18),\n"
            "    \"year\" smallint,\n"
            "    duration numeric(9, 5),\n"
            "    CHECK (\"year\" >= 0 AND \"year\" <= 2999),\n"
            "    CHECK (duration > 0),\n"
            "    FOREIGN KEY (artist_id)\n"
            "    REFERENCES artists (artist_id) ON DELETE SET NULL"
            ");\n"
        )

        # Fact Table 'songplay'
        songplay_table_create = (
            "CREATE TABLE IF NOT EXISTS songplay\n"
            "(\n"
            "    songplay_id serial PRIMARY KEY,\n"
            "    start_time timestamp NOT NULL,\n"
            "    user_id int,\n"
            "    level varchar(32),\n"
            "    song_id char(18),\n"
            "    artist_id char(18),\n"
            "    session_id int,\n"
            "    location varchar(512),\n"
            "    user_agent text,\n"
            "    CHECK (session_id > 0),\n"
            "    CHECK (user_id > 0),\n"
            "    FOREIGN KEY (start_time)\n"
            "    REFERENCES \"time\" (start_time),\n"
            "    FOREIGN KEY (user_id)\n"
            "    REFERENCES users (user_id) ON DELETE SET NULL,\n"
            "    FOREIGN KEY (song_id)\n"
            "    REFERENCES songs (song_id) ON DELETE SET NULL,\n"
            "    FOREIGN KEY (artist_id)\n"
            "    REFERENCES artists (artist_id) ON DELETE SET NULL\n"
            ");\n"
        )

        # DROP TABLES
        songplay_table_drop = ("DROP TABLE IF EXISTS songplay;\n")
        time_table_drop = ("DROP TABLE IF EXISTS \"time\";\n")
        user_table_drop = ("DROP TABLE IF EXISTS users;\n")
        song_table_drop = ("DROP TABLE IF EXISTS songs;\n")
        artist_table_drop = ("DROP TABLE IF EXISTS artists;\n")



        # QUERY LISTS
        self.create_table_queries = [
            time_table_create,
            user_table_create,
            artist_table_create,
            song_table_create,
            songplay_table_create
        ]

        self.drop_table_queries = [
            songplay_table_drop,
            time_table_drop,
            user_table_drop,
            song_table_drop,
            artist_table_drop
        ]

    @staticmethod
    def artist_table_insert(dataframe, verbose=False):
        """Query to insert data from dataframe 'songs'.

        Insert the required data in the table 'artists'.

        Parameters
        ----------
        dataframe : Pandas Dataframe
            description -> Dataframe with the artist data
            format -> Headers: [
                "artist_id",
                "artist_name",
                "artist_location",
                "artist_latitude",
                "artist_longitude"
            ]
            options -> No apply

        verbose : bool
            description -> Print process workflow or results, useful for
                debugging
            format -> No apply
            options -> No apply

        Returns
        -------
        query : string
            description -> The complete SQL statement
            format -> No apply
            options -> No apply
        """
        dataframe = str(dataframe.values.tolist())[1:-1]
        dataframe = re.sub(r'\[', r'    (', dataframe)
        dataframe = re.sub(r'\]', r')', dataframe)
        dataframe = re.sub(r'\), ', r'),\n', dataframe)
        dataframe = re.sub(r'nan|\'nan\'', r'NULL', dataframe)
        dataframe = re.sub("<single_quote_tag>", "''", dataframe)
        dataframe += '\n'
        query = (
            "INSERT INTO artists\n"
            "(artist_id, \"name\", location, latitude, longitude)\n"
            "VALUES\n"
            f"{dataframe}"
            "ON CONFLICT (artist_id)\n"
            "DO UPDATE SET\n"
            "\"name\" = EXCLUDED.\"name\",\n"
            "location = EXCLUDED.location,\n"
            "latitude = EXCLUDED.latitude,\n"
            "longitude = EXCLUDED.longitude;\n"
        )

        if verbose:
            print(f"SQL statement:\n{query}\n")

        return query

    @staticmethod
    def song_table_insert(dataframe, verbose=False):
        """Query to insert data from dataframe 'songs'.

        Insert the required data in the table 'songs'.

        Parameters
        ----------
        dataframe : Pandas Dataframe
            description -> Dataframe with the songs data
            format -> Headers: [
                "song_id",
                "title",
                "artist_id",
                "year",
                "duration"
            ]
            options -> No apply

        verbose : bool
            description -> Print process workflow or results, useful for
                debugging
            format -> No apply
            options -> No apply

        Returns
        -------
        query : string
            description -> The complete SQL statement
            format -> No apply
            options -> No apply
        """
        dataframe = str(dataframe.values.tolist())[1:-1]
        dataframe = re.sub(r'\[', r'    (', dataframe)
        dataframe = re.sub(r'\]', r')', dataframe)
        dataframe = re.sub(r'\), ', r'),\n', dataframe)
        dataframe = re.sub(r'nan|\'nan\'', r'NULL', dataframe)
        dataframe = re.sub("<single_quote_tag>", "''", dataframe)
        dataframe += '\n'
        query = (
            "INSERT INTO songs\n"
            "(song_id, title, artist_id, \"year\", duration)\n"
            "VALUES\n"
            f"{dataframe}"
            "ON CONFLICT (song_id)\n"
            "DO UPDATE SET\n"
            "title = EXCLUDED.title,\n"
            "artist_id = EXCLUDED.artist_id,\n"
            "\"year\" = EXCLUDED.\"year\",\n"
            "duration = EXCLUDED.duration;\n"
        )

        if verbose:
            print(f"SQL statement:\n{query}\n")

        return query

    @staticmethod
    def time_table_insert(dataframe, verbose=False):
        """Query to insert data from dataframe 'logs'.

        Insert the required data in the table 'time'.

        Parameters
        ----------
        dataframe : Pandas Dataframe
            description -> Dataframe with the time data
            format -> Headers: [
                "start_time",
                "hour",
                "day",
                "week",
                "month",
                "year",
                "weekday"
            ]
            options -> No apply

        verbose : bool
            description -> Print process workflow or results, useful for
                debugging
            format -> No apply
            options -> No apply

        Returns
        -------
        query : string
            description -> The complete SQL statement
            format -> No apply
            options -> No apply
        """
        dataframe = str(dataframe.values.tolist())[1:-1]
        dataframe = re.sub(r'\[', r'    (', dataframe)
        dataframe = re.sub(r'\]', r')', dataframe)
        dataframe = re.sub(r'\), ', r'),\n', dataframe)
        dataframe += '\n'
        query = (
            "INSERT INTO time\n"
            "(start_time, hour, day, week, month, year, weekday)\n"
            "VALUES\n"
            f"{dataframe}"
            "ON CONFLICT (start_time)\n"
            "DO UPDATE SET\n"
            "hour = EXCLUDED.hour,\n"
            "day = EXCLUDED.day,\n"
            "week = EXCLUDED.week,\n"
            "month = EXCLUDED.month,\n"
            "year = EXCLUDED.year,\n"
            "weekday = EXCLUDED.weekday;\n"
        )

        if verbose:
            print(f"SQL statement:\n{query}\n")

        return query

    @staticmethod
    def user_table_insert(dataframe, verbose=False):
        """Query to insert data from dataframe 'logs'.

        Insert the required data in the table 'users'.

        Parameters
        ----------
        dataframe : Pandas Dataframe
            description -> Dataframe with the uer data
            format -> Headers: [
                "user_id",
                "first_name",
                "last_name",
                "gender",
                "level"
            ]
            options -> No apply

        verbose : bool
            description -> Print process workflow or results, useful for
                debugging
            format -> No apply
            options -> No apply

        Returns
        -------
        query : string
            description -> The complete SQL statement
            format -> No apply
            options -> No apply
        """
        dataframe = str(dataframe.values.tolist())[1:-1]
        dataframe = re.sub(r'\[', r'    (', dataframe)
        dataframe = re.sub(r'\]', r')', dataframe)
        dataframe = re.sub(r'\), ', r'),\n', dataframe)
        dataframe = re.sub("<single_quote_tag>", "''", dataframe)
        dataframe = re.sub(r'nan|\'nan\'', r'NULL', dataframe)
        dataframe += '\n'
        query = (
            "INSERT INTO users\n"
            "(user_id, first_name, last_name, gender, level)\n"
            "VALUES\n"
            f"{dataframe}"
            "ON CONFLICT (user_id)\n"
            "DO UPDATE SET\n"
            "first_name = EXCLUDED.first_name,\n"
            "last_name = EXCLUDED.last_name,\n"
            "gender = EXCLUDED.gender,\n"
            "level = EXCLUDED.level;\n"
        )

        if verbose:
            print(f"SQL statement:\n{query}\n")

        return query

    @staticmethod
    def songplay_table_insert(dataframe, verbose=False):
        """Query to insert data from dataframe 'logs'.

        Insert the required data in the table 'songplay'.

        Parameters
        ----------
        dataframe : Pandas Dataframe
            description -> Dataframe with the uer data
            format -> Headers: [
                "time_start",
                "user_id",
                "level",
                "song_id",
                "artist_id",
                "session_id",
                "location",
                "user_agent"
            ]
            options -> No apply

        verbose : bool
            description -> Print process workflow or results, useful for
                debugging
            format -> No apply
            options -> No apply

        Returns
        -------
        query : string
            description -> The complete SQL statement
            format -> No apply
            options -> No apply
        """
        dataframe = str(dataframe.values.tolist())[1:-1]
        dataframe = re.sub(r'\[', r'    (', dataframe)
        dataframe = re.sub(r'\]', r')', dataframe)
        dataframe = re.sub(r'\), ', r'),\n', dataframe)
        dataframe = re.sub("<single_quote_tag>", "''", dataframe)
        dataframe = re.sub(r'nan|\'nan\'', r'NULL', dataframe)
        dataframe += '\n'
        query = (
            "INSERT INTO songplay (\n"
            "    start_time,\n"
            "    user_id,\n"
            "    level,\n"
            "    song_id,\n"
            "    artist_id,\n"
            "    session_id,\n"
            "    location,\n"
            "    user_agent\n"
            ")\n"
            "VALUES\n"
            f"{dataframe}"
            ";\n"
        )

        if verbose:
            print(f"SQL statement:\n{query}\n")

        return query

    @staticmethod
    def song_select(dataframe, verbose=False):
        """Query to find artist IDs and song IDs by names and song duration.

        Input the artist name, song title, and song length; the query is
        designed to return the artist ID and song ID instead.

        Parameters
        ----------
        dataframe : Pandas Dataframe
            description -> Dataframe with the uer data
            format -> Headers: ["song_name", "artist_name", "length"]
            options -> No apply

        verbose : bool
            description -> Print process workflow or results, useful for
                debugging
            format -> No apply
            options -> No apply

        Returns
        -------
        query : string
            description -> The complete SQL statement to load IDs
            format -> No apply
            options -> No apply
        """
        dataframe = str(dataframe.values.tolist())[1:-1]
        dataframe = re.sub(r'\[', r'        (', dataframe)
        dataframe = re.sub(r'\]', r')', dataframe)
        dataframe = re.sub(r'\), ', r'),\n', dataframe)
        dataframe = re.sub("<single_quote_tag>", "''", dataframe)
        dataframe = re.sub(r'nan|\'nan\'', r'NULL', dataframe)
        dataframe += '\n'
        query = (
            "SELECT\n"
            "    song_artist.song_id song, --input_data.song\n"
            "    song_artist.artist_id artist --input_data.artist\n"
            "FROM (\n"
            "    SELECT idx, song, artist, duration\n"
            "    FROM (\n"
            "        VALUES\n"
            f"{dataframe}"
            "    ) AS headers (idx, song, artist, duration)\n"
            ") AS input_data\n"
            "LEFT JOIN (\n"
            "    SELECT\n"
            "        songs.song_id,\n"
            "        songs.title,\n"
            "        songs.duration,\n"
            "        artists.artist_id,\n"
            "        artists.\"name\"\n"
            "    FROM songs\n"
            "    LEFT JOIN artists\n"
            "    ON songs.artist_id = artists.artist_id\n"
            ") song_artist\n"
            "ON input_data.song = song_artist.title\n"
            "AND input_data.artist = song_artist.\"name\"\n"
            "AND input_data.duration = song_artist.duration\n"
            "ORDER BY input_data.idx ASC;\n"
        )

        if verbose:
            print(f"SQL statement:\n{query}\n")

        return query
