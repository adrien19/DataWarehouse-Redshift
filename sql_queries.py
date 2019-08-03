import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs cascade"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar, 
        auth varchar, 
        firstName varchar, 
        gender varchar, 
        iteminSession int, 
        lastName varchar, 
        length float, 
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration float,
        sessionId int,
        song varchar,
        status int,
        ts bigint,
        userAgent varchar,
        userId int
   );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int, 
        artist_id varchar, 
        artist_latitude float, 
        artist_longitude float, 
        artist_location varchar, 
        artist_name varchar, 
        song_id varchar, 
        title varchar, 
        duration float,
        year int
   );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0,1) PRIMARY KEY not null sortkey distkey, 
        start_time bigint, 
        user_id int, 
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id int, 
        location text, 
        user_agent text
   );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id varchar sortkey, 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY not null sortkey, 
        title varchar, 
        artist_id varchar, 
        year int, 
        duration float
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY not null sortkey, 
        name varchar, 
        location text, 
        latitude float, 
        longitude float
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time bigint not null sortkey, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        week_day int
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role {}
    compupdate off region 'us-west-2'
    JSON {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = (""" 
    copy staging_songs
    from {}
    iam_role {}
    compupdate off region 'us-west-2'
    JSON 'auto' truncatecolumns;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent)
        SELECT events.start_time, events.userId, events.level, songs.song_id, songs.artist_id, events.sessionId, events.location, events.useragent 
        FROM (SELECT  ts AS start_time, *
          FROM staging_events
          WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
        AND events.artist = songs.artist_name
        AND events.length = songs.duration
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level) 
        SELECT DISTINCT userId, firstName, lastName, gender, level 
        FROM staging_events;
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id, 
        title, 
        artist_id, 
        year, 
        duration) 
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude)
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        week_day)
        SELECT DISTINCT ts
        ,EXTRACT(HOUR FROM start_time) As hour
        ,EXTRACT(DAY FROM start_time) As day
        ,EXTRACT(WEEK FROM start_time) As week
        ,EXTRACT(MONTH FROM start_time) As month
        ,EXTRACT(YEAR FROM start_time) As year
        ,EXTRACT(DOW FROM start_time) As weekday
        FROM (
        SELECT distinct ts,'1970-01-01'::date + ts/1000 * interval '1 second' as start_time
        FROM staging_events
        );
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
