import configparser
import psycopg2
import boto3

"""
    Lambda function for ETL data quality checks
    Publishes SNS message if any check(s) do not pass
"""

def main(event, context):
    ok = True

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # check cows
    cur.execute("SELECT COUNT(*) FROM staging_cows")
    row = cur.fetchone()
    data, staging_cows = row[:-1], row[-1]

    cur.execute("SELECT COUNT(*) FROM cows")
    row = cur.fetchone()
    data, cows = row[:-1], row[-1]

    if (staging_cows == 0):
        ok = False

    if (staging_cows != cows):
        ok = False

    # check sensors
    cur.execute("SELECT COUNT(*) FROM staging_sensors")
    row = cur.fetchone()
    data, staging_sensors = row[:-1], row[-1]

    cur.execute("SELECT COUNT(*) FROM sensors")
    row = cur.fetchone()
    data, sensors = row[:-1], row[-1]

    if (staging_sensors == 0):
        ok = False

    if (staging_sensors != sensors):
        ok = False

    # check sensor readings
    cur.execute("SELECT COUNT(*) FROM staging_events")
    row = cur.fetchone()
    data, staging_events = row[:-1], row[-1]

    cur.execute("SELECT COUNT(*) FROM sensorreadings")
    row = cur.fetchone()
    data, sensorreadings = row[:-1], row[-1]

    if (staging_events == 0):
        ok = False

    if (staging_events > sensorreadings):
        ok = False

    conn.close()

    if (not ok):
        # publish SNS message
        client = boto3.client('sns')
        topic = client.create_topic(Name='message-from-lambda')
        topic_arn = topic['TopicArn'] 
        client.publish(Message="Warning: possible ETL data quality issue.  Please investigate.", TopicArn=topic_arn)