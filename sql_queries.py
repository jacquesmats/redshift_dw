import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events(
        event_id       int            identity(0,1)    not null,
        artist         varchar,
        auth           varchar,
        first_name     varchar,
        gender         varchar,
        item_session   integer,
        last_name      varchar,
        length         float,
        level          varchar,
        location       varchar,
        method         varchar,
        page           varchar,
        registration   varchar,
        session_id     integer,
        song           varchar DISTKEY,
        status         integer,
        ts             bigint SORTKEY,
        user_agent     varchar,
        user_id        integer 
    );
""")

staging_songs_table_create = (""" 
    CREATE TABLE staging_songs(
        num_songs           integer,
        artist_id           varchar,
        artist_latitude     float,
        artist_longitude    float,
        artist_location     varchar,
        artist_name         varchar,
        song_id             varchar,
        title               varchar DISTKEY,
        duration            float,
        year                integer                
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id    integer         identity(0,1)    not null,
        start_time     timestamp       not null SORTKEY,
        user_id        integer         not null,
        level          varchar      not null,
        song_id        varchar         not null DISTKEY,
        artist_id      varchar         not null,
        session_id     integer         not null,
        location       varchar,
        user_agent     varchar            
    );
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id        integer         not null SORTKEY,
        first_name     varchar     not null,
        last_name      varchar     not null,
        gender         varchar      not null,
        level          varchar      not null    
    );
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id        varchar         not null DISTKEY,
        title          varchar         not null,
        artist_id      varchar         not null,
        year           integer         not null SORTKEY,
        duration       float           not null    
    );
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id      varchar         not null SORTKEY,
        name           varchar,
        location       varchar,
        lattitude      float,
        longitude      float    
    );
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time     timestamp       not null SORTKEY,
        hour           integer         not null,
        day            integer         not null,
        week           integer         not null,
        month          integer         not null,
        year           integer         not null,
        weekday        integer         not null       
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    FORMAT AS JSON '{}'
    """).format(config.get('S3','LOG_DATA'), \
                config.get('IAM_ROLE', 'ARN'),
                config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    FORMAT AS JSON 'auto'
    """).format(config.get('S3','SONG_DATA'), 
                config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
        e.user_id, 
        e.level, 
        s.song_id, 
        s.artist_id, 
        e.session_id, 
        e.location,
        e.user_agent
    FROM staging_events e
        JOIN staging_songs s ON e.song = s.title 
                             AND e.artist = s.artist_name
    WHERE e.page = 'NextSong'
        AND user_id NOT IN (SELECT DISTINCT s.user_id FROM songplays s WHERE s.user_id = user_id
                            AND s.start_time = start_time AND s.session_id = session_id )
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        user_id, first_name, last_name, gender, level
    FROM staging_events
    WHERE page = 'NextSong'
    AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)

""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, lattitude, longitude)
    SELECT DISTINCT
        artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time,
           EXTRACT(hour FROM start_time),
           EXTRACT(day FROM start_time),
           EXTRACT(week FROM start_time),
           EXTRACT(month FROM start_time),
           EXTRACT(year FROM start_time),
           EXTRACT(weekday FROM start_time)
    FROM songplays
    WHERE start_time IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
