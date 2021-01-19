---------------------------------------------------------------
-- SQL for creating external schema and table for Redshift Spectrum
-- enables querying parquet files in a data lake on S3
-- https://docs.aws.amazon.com/redshift/latest/dg/c-getting-started-using-spectrum-create-external-table.html
---------------------------------------------------------------

CREATE EXTERNAL SCHEMA spectrum 
FROM DATA CATALOG
DATABASE 'datalakedb' 
iam_role 
CREATE EXTERNAL DATABASE IF NOT EXISTS;

CREATE EXTERNAL TABLE datalakedb.sensor_avg
(
    sensor_id VARCHAR(8),
    reading_date TIMESTAMP,
    avg_temp DOUBLE,
    avg_motion DOUBLE,
    avg_rumination DOUBLE
)
STORED AS PARQUET
LOCATION 
TBLPROPERTIES ('classification'='parquet');