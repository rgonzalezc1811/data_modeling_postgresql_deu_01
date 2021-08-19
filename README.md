# **README** <!-- omit in toc -->
# Data Modeling with PostgreSQL <!-- omit in toc -->

<!-- Original image size [500 457] -->
<div align="center">
  <img width="250" height="229" src="img/postgresql_logo.png"/>
</div>
<br/><br/> <!-- Blank line -->

Model user activity data to create a database and **ETL** (**Extract,
Trandform, Load**) pipeline in PostgreSQL for a music streaming app and
define a Fact and Dimension tables.

## **Table of Content** <!-- omit in toc -->
- [Introduction](#introduction)
- [Project Description](#project-description)
- [Project Development](#project-development)
  - [Datasets](#datasets)
    - [Download the Datasets](#download-the-datasets)
    - [Dataset Tree Directory](#dataset-tree-directory)
    - [Song Dataset](#song-dataset)
    - [Log Dataset](#log-dataset)
    - [Entity Relational Diagram (ERD)](#entity-relational-diagram-erd)
- [Development](#development)
  - [Database](#database)
    - [Schema](#schema)
    - [Local Database Setup](#local-database-setup)
  - [Files Description](#files-description)
  - [Tables Creation](#tables-creation)
  - [ETL Process](#etl-process)
  - [ETL Pipeline](#etl-pipeline)
  - [Results](#results)
  - [Discussion](#discussion)

<br/><br/> <!-- Blank line -->

# Introduction

A startup called _**Sparkify**_ wants to analyze the data they have been
collecting on songs and user activity on their new music streaming app.
The analytics team is particularly interested in understanding **what
songs users are listening to**. Currently, they do not have an easy way
to query the **data**, which **resides in a directory of JSON logs** on
user activity on the app, as well as a **directory with JSON metadata on
the songs** in their app.

It is required to a **PostgresSQL database** with tables designed to
optimize queries on song play analysis **creating a database schema**
and **ETL pipeline** for this analysis. 

The database and ETL process are requited to be tested by running given
queries by the analytics team from Sparkify and comparing the outputs
with the expected results.

<br/><br/> <!-- Blank line -->

# Project Description

In the project is applied knowledge about **data modeling** with
PostgreSQL and **ETL pipeline** using Python. **Fact** and **Dimension**
tables are designed in a **star schema** for a particular analytic
focus, and the ETL pipeline **transfers data from files** in two local
directories into the mentioned **tables** in PostgreSQL using Python and
SQL.

<br/><br/> <!-- Blank line -->

# Project Development

To develop all the project, it is already defined the Schema, but no the
complete database structure, also, the raw data is only available in
`JSON` files, so a ETL pipeline is required, the following sections
describe how the project is handled.

<br/><br/> <!-- Blank line -->

## Datasets

To understand the requirements, business purposes and technical
characteristics of the project the first step is get the datasets into
the development environment.

<br/><br/> <!-- Blank line -->

### Download the Datasets

1. Access to Udacity's Project Workspace and open a `Terminal`.

<!-- Original image size [1369 820] -->
<div align="center">
  <img width="547" height="328" src="img/udacity_workspace_1.png" />
</div>
<br/><br/> <!-- Blank line -->

2. Inside the terminal type the following command:
   ```bash
   zip -r data.zip data
   ```

3. In the workspace directory (`/home/workspace`) a new file will appear
   and it is called `data.zip`, download the file.

   <!-- Original image size [822 632] -->
<div align="center">
  <img width="493" height="379" src="img/data_zip.png" />
</div>
<br/><br/> <!-- Blank line -->

4. On this case, the `data.zip` is also available in a personal
   [drive](https://drive.google.com/file/d/1TfPWGYwNFL_Y8t21z3kUL6s7B0sBt2EF/view?usp=sharing)
   to download the datasets.

<br/><br/> <!-- Blank line -->

### Dataset Tree Directory

Locate the `data` directory in the root path of the development
environment, all `JSON` files are located here, the following diagram
shows the current directory tree.

```bash
<root_path>/data_modeling_postgresql_deu_01/data
├───log_data
│   └───2018
│       └───11
└───song_data
    └───A
        ├───A
        │   ├───A
        │   ├───B
        │   └───C
        └───B
            ├───A
            ├───B
            └───C
```
<br/><br/> <!-- Blank line -->

### Song Dataset

Subset of real data from the **Million Song Dataset**. Each file is in
`JSON` format and contains _metadata_ about a song and the artist of
that song. The files are partitioned by the first three letters of each
song's track ID. Below are the root tree directory and filepath examples
of two files in the dataset.

```bash
<root_path>/data_modeling_postgresql_deu_01/data/song_data
└───A
   ├───A
   │   ├───A
   │   ├───B
   │   └───C
   └───B
      ├───A
      ├───B
      └───C
```


* `<root_path>/data_modeling_postgresql_deu_01/data/song_data/A/A/B/TRAABCL128F4286650.json`
* `<root_path>/data_modeling_postgresql_deu_01/data/song_data/A/B/C/TRABCYE128F934CE1D.json`

Below is an example of what a single song file looks like
(`TRABCAJ12903CDFCC2.json`):

```JSON
{
  "num_songs": 1,
  "artist_id": "ARULZCI1241B9C8611",
  "artist_latitude": null,
  "artist_longitude": null,
  "artist_location": "",
  "artist_name": "Luna Orbit Project",
  "song_id": "SOSWKAV12AB018FC91",
  "title": "Midnight Star",
  "duration": 335.51628,
  "year": 0
}
```

It is important to identify what are the content of the available files,
sometimes, the name of the attributes helps to know the type of data to
store, for example, the `duration` attribute and current value indicates
that is not possible to have sentences here, so at designing the
database a good type of data could be `float` or `numeric`.

It is a recommendation to **visualize more than one** `JSON` file, it
helps to understand data better and identify possible inconsistencies or
common missing values.

<br/><br/> <!-- Blank line -->

### Log Dataset

Consists of **log files** in `JSON` format generated by this event
simulator based on the songs in the dataset songs. These simulate
activity logs from a music streaming app based on specified
configurations.  Below are the root tree directory and filepath examples
of two files in the dataset,  the log files in the dataset are
partitioned by year and month.

```bash
<root_path>/data_modeling_postgresql_deu_01/data/log_data
├───log_data
    └───2018
        └───11
```

* `<root_path>/data_modeling_postgresql_deu_01/data/log_data/2018/11/2018-11-12-events.json`
* `<root_path>/data_modeling_postgresql_deu_01/data/log_data/2018/11/2018-11-13-events.json`

To look at the `JSON` data within `log_data` files, create a pandas
dataframe to read the data (code example below), after the code block
example, there is an example of what the data in a log file
(`2018-11-12-events.json`) looks like.

```Python
import json
import os
import pandas as pd

song_log_path = os.path.join(
    os.getcwd(), "data/log_data/2018/11/2018-11-01-events.json"
)
song_logs_df = pd.read_json(song_log_path, lines=True)
songs_logs_df.head()
```

<!-- Original image size [1525 460] -->
<div align="center">
  <img width="915" height="276" src="img/log_data_example.png" />
</div>
<br/><br/> <!-- Blank line -->

As mentioned in the previous section, it is important to identify what
are the content of the available files, the name of the attributes helps
to know the type of data to store, for example, the `page` attribute
and values indicates that is possible to have only specific values and
no numbers, just strings, so at designing the database a good type of
data could be `varchar` or `text`.

It is a recommendation to **visualize more than one** `JSON` file, it
helps to understand data better and identify possible inconsistencies or
common missing values.

> **Comments**:
> 
> * Use this
>   [JSON file format (video)](https://www.youtube.com/watch?time_continue=1&v=hO2CayzZBoA).
>   resource to better understand the JSON files.
> * HINT: Use the `value_counts` method on log dataframes, this is a
>   good option to identify attributes that store only specific values,
>   e.g:
> 
>       `df["attribute_n"].value_counts()`
> <br/><br/> <!-- Blank line -->
> 
<br/><br/> <!-- Blank line -->

### Entity Relational Diagram (ERD)

The `Star Schema` designed allows to create queries easily to retrieve
specific business metrics, the Sparkify's ERD is showed bewllow.

<!-- Original image size [1448 1053] -->
<br/><br/> <!-- Blank line -->
<div align="center">
  <img width="868" height="632" src="img/erd.png"/>
</div>
<br/><br/> <!-- Blank line -->


<br/><br/> <!-- Blank line -->



# Development

This section contains general description and notations for the Data
Modeling development.
<br/><br/> <!-- Blank line -->

## Database

The following section indicates how the database is configured.
<br/><br/> <!-- Blank line -->

### Schema

Using the `song_data` and `log_data` datasets, **star schema** schema
is created, optimized for queries on song play analysis.
<br/><br/> <!-- Blank line -->

**Fact Table**

1. **songplay**: Records in log data associated with song plays i.e.
   records with page `NextSong`.

     * songplay_id
     * start_time
     * user_id
     * level
     * song_id 
     * artist_id
     * session_id
     * location
     * user_agent
     <br/><br/> <!-- Blank line -->

**Dimension Tables**

1. **users**: Users in the app.

    * user_id
    * first_name
    * last_name
    * gender
    * level
    <br/><br/> <!-- Blank line -->

2. **songs**: Songs in music database.

    * song_id
    * title
    * artist_id
    * year
    * duration
    <br/><br/> <!-- Blank line -->

3. **artists**: Artists in music database.

    * artist_id
    * name
    * location
    * latitude
    * longitude
    <br/><br/> <!-- Blank line -->

4. **time**: Timestamps of records in **songplay** broken down into specific
   units.

    * start_time
    * hour
    * day
    * week
    * month
    * year
    * weekday
    <br/><br/> <!-- Blank line -->



### Local Database Setup

1. Make sure the PostgreSQL proram is installed.
2. Open `SQL Shell`, use the **Search** tool and type `psql`.
3. Access under the following inputs:
   1. Server: localhost
   2. Database: postgres
   3. Port: 5432
   4. Username: postgres
   5. Password: <Password_configured_at_installing_PostgreSQL>
4. Verify if the `admin` does not exists, if the role exists, go to step
   number _X_:
   ```bash
   \du
   ```
5. Create the `admin` role:
   ```bash
   CREATE ROLE admin WITH LOGIN ENCRYPTED PASSWORD '<password>';
   ```
6. Assign permissions to the created role:
   ```bash
   ALTER ROLE admin CREATEDB;
   ```
7. Quit the `SQL SHELL`
   ```bash
   \q
   ```
8. Open again the `SQL Shell`.
9. Access as `admin`, use created password>
   1. Server: localhost
   2. Database: postgres
   3. Port: 5432
   4. Username: admin
   5. Password: <Password_configured_at_creating_role_admin>
10. Create the database:
    ```bash
    CREATE DATABASE data_modeling_postgres_01;
    ```
11. Quit `SQL Shell`.

## Files Description

The development include the completion of specific scripts:

1. `test.ipynb`: Displays the first few rows of each table to check the
   database.
   <br><br> <!-- Blank line -->

2. `create_tables.py`: Drops and creates the tables. Running this file
   resets the tables before each time to run the **ETL** scripts.
   <br><br> <!-- Blank line -->

3. `etl.ipynb`: Reads and processes a single file from **song_data** and
   **log_data** and loads the data into the tables. This notebook
   contains detailed **instructions** on the **ETL process** for each of
   the tables.
   <br><br> <!-- Blank line -->

4. `etl.py`: Reads and processes files from **song_data** and
   **log_data** and loads them into the tables. Based on the work in the
   ETL notebook.
   <br><br> <!-- Blank line -->

5. `sql_queries.py`: Contains all the **SQL queries**, and is imported
   into the last three files above.
   <br><br> <!-- Blank line -->

## Tables Creation
  1. Write `CREATE` statements in `sql_queries.py` file to create each
     table.
  2. Write `DROP` statements in `sql_queries.py` file to drop each table
     if it exists.
  3. Run `create_tables.py` to create the database and tables.
  4. Run `test.ipynb` to confirm the creation of the tables with the
     correct columns. Make sure to close always the connection to the
     database after finishing working in the project.
  <br><br> <!-- Blank line -->

## ETL Process

Follow instructions in the `etl.ipynb` notebook to develop ETL processes
for each table. At the end of each table section, or at the end of the
notebook, run `test.ipynb` to confirm that records were successfully
inserted into each table. Remember to re-run `create_tables.py` to reset
the tables before each time running the notebook.
<br><br> <!-- Blank line -->

## ETL Pipeline

Use `etl.ipynb` to complete `etl.py`, where to process the entire
datasets. Run `create_tables.py` before running `etl.py` to reset the
tables.

Run `test.ipynb` to confirm the records were successfully inserted into
each table.
<br><br> <!-- Blank line -->

## Results

The following images shows the final tables content after executing the
finished `etl.py` script.

<!-- Original image size [1279 428] -->
<br/><br/> <!-- Blank line -->
<div align="center">
  <img width="895" height="300" src="img/artists_full.png"/>
</div>
<br/><br/> <!-- Blank line -->

<!-- Original image size [1282 447] -->
<br/><br/> <!-- Blank line -->
<div align="center">
  <img width="897" height="313" src="img/songs_full.png"/>
</div>
<br/><br/> <!-- Blank line -->

<!-- Original image size [1083 334] -->
<br/><br/> <!-- Blank line -->
<div align="center">
  <img width="758" height="234" src="img/time_full.png"/>
</div>
<br/><br/> <!-- Blank line -->

<!-- Original image size [1078 374] -->
<br/><br/> <!-- Blank line -->
<div align="center">
  <img width="755" height="262" src="img/users_full.png"/>
</div>
<br/><br/> <!-- Blank line -->

<!-- Original image size [1340 505] -->
<br/><br/> <!-- Blank line -->
<div align="center">
  <img width="938" height="354" src="img/songplays_full.png"/>
</div>
<br/><br/> <!-- Blank line -->

## Discussion

The current database schema is focused on how frequent an user uses the
platform and identify which songs and artist are frequently heard.

Based on the `sparkify` database, it is possible to retrieve different
metrics, some of them are:

1. How many users pays for the platform use? 

```sql
SELECT u."level", COUNT(u."level")
FROM users u
GROUP BY u."level";
```

Query result:

| level       | count       |
| ----------- | ----------- |
| free        | 74          |
| paid        | 22          |

The intention is users paid for the use of the platform, and the metrics
shows that less than 25% of users pay, comparing the number of paid
licenses per month could indicates if a marketing campaign is working or
not.

2. What are the most frequent hours the users use the platform?

```sql
SELECT t."hour", COUNT(t."hour") AS frequency
FROM songplay sp
JOIN "time" t
ON sp.start_time = t.start_time
GROUP BY t."hour"
ORDER BY frequency DESC
LIMIT 5;
```

Query result:
| hour        | frequency   |
| ----------- | ----------- |
| 16          | 542         |
| 18          | 498         |
| 17          | 494         |
| 15          | 477         |
| 14          | 432         |

This could help to decide at which hours put some marketing or
promotions to free users.

**About ETL pipeline**

The `ETL` pipeline could have some issues, for example, as seeing in the
image results, there a lot of missing artists and songs IDs, at this
point it is required to speak with Sparkify's responsibles, because it
is important to know how to assing those IDs, using an arbitrary ID?
waiting until new songs data files appears?

> <br><br> <!-- Blank line -->
>
