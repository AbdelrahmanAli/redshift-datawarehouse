import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist        VARCHAR,
    auth          VARCHAR,
    first_name     VARCHAR,
    gender        CHAR,
    itemInSession INT,
    last_name      VARCHAR,
    length        DECIMAL,
    level         VARCHAR,
    location      VARCHAR,
    method        VARCHAR,
    page          VARCHAR,
    registeration DECIMAl,
    session_id     INT,
    song          VARCHAR,
    status        INT,
    ts            BIGINT,
    user_agent     VARCHAR,
    user_id        INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_songs        INT,
    artist_id        VARCHAR, 
    artist_latitude  DECIMAL,
    artist_longitude DECIMAL,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         DECIMAL,
    year             INT
);
""")

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id BIGINT IDENTITY,
    start_time  TIMESTAMP NOT NULL SORTKEY, 
    user_id     INT,
    level       VARCHAR, 
    song_id     VARCHAR DISTKEY, 
    artist_id   VARCHAR, 
    session_id  INT NOT NULL, 
    location    VARCHAR, 
    user_agent  VARCHAR,
    PRIMARY KEY(songplay_id)
);
""")

user_table_create = ("""
CREATE TABLE users(
    user_id    INT SORTKEY, 
    first_name VARCHAR NOT NULL, 
    last_name  VARCHAR NOT NULL, 
    gender     VARCHAR NOT NULL, 
    level      VARCHAR NOT NULL,
    PRIMARY KEY(user_id)
)
DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id   VARCHAR SORTKEY, 
    title     VARCHAR NOT NULL, 
    artist_id VARCHAR, 
    year      INT NOT NULL, 
    duration  DECIMAL NOT NULL,
    PRIMARY KEY(song_id)
)
DISTSTYLE ALL;
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id VARCHAR SORTKEY, 
    name      VARCHAR NOT NULL, 
    location  VARCHAR, 
    latitude  DECIMAL, 
    longitude DECIMAL,
    PRIMARY KEY(artist_id)
)
DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE time(
    start_time TIMESTAMP SORTKEY, 
    hour       INT NOT NULL, 
    day        INT NOT NULL, 
    week       INT NOT NULL, 
    month      INT NOT NULL, 
    year       INT NOT NULL, 
    weekday    VARCHAR NOT NULL,
    PRIMARY KEY(start_time)
)
DISTSTYLE ALL;
""")

# STAGING TABLES
staging_events_copy = ("""
                        COPY staging_events 
                        FROM {}
                        iam_role {} 
                        JSON {}
                        """).format(
                            config.get('S3','LOG_DATA'),
                            config.get('IAM_ROLE','ARN'),
                            config.get('S3','LOG_JSONPATH')
                        )

staging_songs_copy = ("""
                        COPY staging_songs 
                        FROM {}
                        iam_role {}
                        JSON 'auto'
                        """).format(
                            config.get('S3','SONG_DATA'),
                            config.get('IAM_ROLE','ARN')
                        )

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT (timestamp 'epoch' + t1.ts/1000 * interval '1 second') as start_time,
        t1.user_id,
        t1.level,
        t2.song_id,
        t2.artist_id,
        t1.session_id,
        t1.location,
        t1.user_agent
FROM staging_events AS t1
JOIN staging_songs AS t2 ON t1.song = t2.title AND t1.artist = t2.artist_name
WHERE t1.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(user_id,first_name,last_name,gender,level)
SELECT  t1.user_id,
        t1.first_name,
        t1.last_name,
        t1.gender,
        t1.level
FROM ( 
        SELECT * , (ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY ts DESC)) AS Row_Number
        FROM staging_events
        WHERE user_id IS NOT NULL
    ) AS t1
WHERE t1.Row_Number = 1
;
""")

song_table_insert = ("""
INSERT INTO songs(song_id,title,artist_id,year,duration)
SELECT  DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id,name,location,latitude,longitude)
SELECT  DISTINCT artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
FROM staging_songs;

""")

time_table_insert = ("""
INSERT INTO time(start_time,hour,day,week,month,year,weekday)
SELECT  DISTINCT start_time, 
        EXTRACT(HOUR FROM start_time) as hour, 
        EXTRACT(DAY FROM start_time) as day, 
        EXTRACT(WEEK FROM  start_time) as week, 
        EXTRACT(MONTH FROM start_time) as month, 
        EXTRACT(YEAR FROM start_time) as year, 
        to_char(start_time,'Day') as weekday
FROM songplays;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

