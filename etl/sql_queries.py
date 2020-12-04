import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE_ARN = config.get('IAM_ROLE','ARN')
EVENT_DATA = config.get('S3','EVENT_DATA')
COW_DATA = config.get('S3','COW_DATA')
SENSOR_DATA = config.get('S3','SENSOR_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_cows_table_drop = "DROP TABLE IF EXISTS staging_cows;"
staging_sensors_table_drop = "DROP TABLE IF EXISTS staging_sensors;"
sensorevents_table_drop = "DROP TABLE IF EXISTS sensorevents;"
sensors_table_drop = "DROP TABLE IF EXISTS sensors;"
cows_table_drop = "DROP TABLE IF EXISTS cows;"
time_table_drop = "DROP TABLE IF EXISTS time;"
groups_table_drop = "DROP TABLE IF EXISTS groups;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        timestamp           integer,
        body_temperature    float,
        motion              integer,
        rumination          float,
        sensor_id           varchar,
        event_id            varchar
    );
""")

staging_cows_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_cows(
        cow_number      integer,
        cow_name        varchar,
        age             integer,
        color           varchar     
    );
""")

staging_sensors_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_sensors(
        sensor_id               varchar,
        sensor_name             varchar,
        description             varchar,
        cow_number              integer,
        group_id                integer,
        group_name              varchar,
        location_description    varchar,
        udder_factor            integer
    );
""")

sensorevents_table_create = ("""
    CREATE TABLE IF NOT EXISTS sensorevents 
    (
        event_id            varchar NOT NULL PRIMARY KEY DISTKEY,
        sensor_id           varchar NOT NULL,
        timestamp           timestamp NOT NULL,
        cow_number          integer NOT NULL,
        group_id            integer,
        body_temperature    float,
        motion              integer,
        rumination          float
    )
    SORTKEY (timestamp, cow_number);
""")

sensors_table_create = ("""
    CREATE TABLE IF NOT EXISTS sensors
    (
        sensor_id       varchar NOT NULL PRIMARY KEY,
        sensor_name     varchar,
        description     varchar,
        cow_number      integer SORTKEY DISTKEY
    );
""")

cows_table_create = ("""
    CREATE TABLE IF NOT EXISTS cows
    (
        cow_number      integer NOT NULL PRIMARY KEY SORTKEY,
        cow_name        varchar,
        age             integer,
        color           varchar
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        timestamp   timestamp NOT NULL PRIMARY KEY SORTKEY DISTKEY,
        month       integer NOT NULL,
        date        integer NOT NULL,
        year        integer NOT NULL,
        hour        integer NOT NULL,
        minute      integer NOT NULL,
        second      integer NOT NULL,
        weekday     integer NOT NULL
    );
""")

groups_table_create = ("""
    CREATE TABLE IF NOT EXISTS groups
    (
        group_id        integer NOT NULL PRIMARY KEY SORTKEY,
        group_name      varchar,
        description     varchar,
        udder_factor    integer   
    );
""")

# DELETE TABLES

staging_events_delete = ("""
    DELETE FROM staging_events
""")

staging_cows_delete = ("""
    DELETE FROM staging_cows
""")

staging_sensors_delete = ("""
    DELETE FROM staging_sensors
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON 'auto'
    timeformat as 'epochsecs'
    TRUNCATECOLUMNS;
""").format(EVENT_DATA, IAM_ROLE_ARN)

staging_cows_copy = ("""
    copy staging_cows from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    CSV
    IGNOREHEADER 1;
""").format(COW_DATA, IAM_ROLE_ARN)

staging_sensors_copy = ("""
    copy staging_sensors from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    CSV
    IGNOREHEADER 1;
""").format(SENSOR_DATA, IAM_ROLE_ARN)

# FINAL TABLES

sensorevents_table_insert = ("""
    INSERT INTO sensorevents (event_id, sensor_id, timestamp, cow_number, group_id, body_temperature, motion, rumination)
    SELECT e.event_id,
        e.sensor_id,
        TIMESTAMP 'epoch' + e.timestamp * INTERVAL '1 Second ' AS timestamp,
        c.cow_number,
        s.group_id,
        e.body_temperature,
        e.motion,
        e.rumination       
    FROM staging_events e INNER JOIN staging_sensors s ON e.sensor_id = s.sensor_id
        INNER JOIN staging_cows c ON c.cow_number = s.cow_number
""")

sensors_table_insert = ("""
    INSERT INTO sensors (sensor_id, sensor_name, description, cow_number)
    SELECT ss.sensor_id,
        ss.sensor_name,
        ss.description,
        ss.cow_number
    FROM staging_sensors ss LEFT JOIN sensors s ON ss.sensor_id = s.sensor_id
    WHERE s.sensor_id IS NULL
""")

cows_table_insert = ("""
    INSERT INTO cows (cow_number, cow_name, age, color)
    SELECT sc.cow_number,
        sc.cow_name,
        sc.age,
        sc.color
    FROM staging_cows sc LEFT JOIN cows c ON sc.cow_number = c.cow_number
    WHERE c.cow_number IS NULL
""")

time_table_insert = ("""
    INSERT INTO time (timestamp, month, date, year, hour, minute, second, weekday)
    SELECT DISTINCT(sr.timestamp),
        EXTRACT(month from sr.timestamp) as month,
        EXTRACT(day from sr.timestamp) as date,
        EXTRACT(year from sr.timestamp) as year,
        EXTRACT(hour from sr.timestamp) as hour,
        EXTRACT(minute from sr.timestamp) as minute,
        EXTRACT(second from sr.timestamp) as second,
        EXTRACT(weekday from sr.timestamp) as weekday
    FROM sensorevents sr LEFT JOIN time t ON sr.timestamp = t.timestamp
    WHERE t.timestamp IS NULL
""")

groups_table_insert = ("""
    INSERT INTO groups (group_id, group_name, description, udder_factor)
    SELECT DISTINCT ss.group_id,
        ss.group_name,
        ss.location_description AS description,
        ss.udder_factor
    FROM staging_sensors ss LEFT JOIN groups l ON ss.group_id = l.group_id
    WHERE l.group_id IS NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_cows_table_create, staging_sensors_table_create, sensorevents_table_create, sensors_table_create, cows_table_create, time_table_create, groups_table_create]
drop_table_queries = [staging_events_table_drop, staging_cows_table_drop, staging_sensors_table_drop, sensorevents_table_drop, sensors_table_drop, cows_table_drop, time_table_drop, groups_table_drop]
delete_table_queries = [staging_events_delete, staging_cows_delete, staging_sensors_delete]
copy_table_queries = [staging_events_copy, staging_cows_copy, staging_sensors_copy]
insert_table_queries = [sensorevents_table_insert, sensors_table_insert, cows_table_insert, time_table_insert, groups_table_insert]