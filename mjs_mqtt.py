import argparse
import base64
import datetime
import time
import json
import logging
import os
import paho.mqtt.client as mqtt
import requests
import ssl
import struct
import MySQLdb
import re
import bitstring

parser = argparse.ArgumentParser()
parser.add_argument(
    '-d', '--debug', action="store_const", dest="loglevel", const=logging.DEBUG,
    default=logging.WARNING,
)
parser.add_argument(
    '-v', '--verbose', action="store_const", dest="loglevel", const=logging.INFO,
)

def on_connect(client, userdata, flags, rc):
    logging.info('Connected to host, subscribing to uplink messages')
    client.subscribe('+/devices/+/up')

def on_message(client, userdata, msg):
    #logging.debug('Received message {}'.format(str(msg.payload)))
    db = userdata['db']

    try:
        msg_as_string = msg.payload.decode('utf8')
        #now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        #message_id = execute_query(db, "INSERT INTO sensors_message SET timestamp = %s, message = %s", (now, msg_as_string))

        message_payload = json.loads(msg_as_string)
        payload = base64.b64decode(message_payload.get('payload_raw', ''))
    # python2 uses ValueError and perhaps others, python3 uses JSONDecodeError
    except Exception as e:
        logging.warn('Error parsing JSON payload')
        logging.warn(e)

    try:
        process_data(db, message_payload, payload)
    except Exception as e:
        logging.warn('Error processing packet')
        logging.warn(e)

def execute_query(db, query, args):
    logging.debug("Executing query: {} with args: {}".format(query, args))

    try:
        # Check if the connection is alive, reconnect if needed
        db.ping(True)
        cursor = db.cursor()
        cursor.execute(query, args)
        cursor.close()
        db.commit()
        return cursor.lastrowid
    except Exception as e:
        logging.warn('Query failed: {}'.format(e))

def process_data(db, message_payload, payload):
    if message_payload["port"] == 20 and (len(payload) < 12):
        logging.warn('Invalid packet received with length {}'.format(len(payload)))
        return

    if message_payload["port"] != 20:
        #logging.warn('Ignoring message with unknown port: {}'.format(message_payload["port"]))
        return

    # TODO: Preserve full id?
    station_id = str(int(message_payload['hardware_serial'], 16))
    now = datetime.datetime.utcnow()

    stream = bitstring.ConstBitStream(bytes=payload)
    time_diff_size, gps_diff_size, sensor_diff_size = stream.readlist('uint:4, uint:4, uint:4')
    print('diff sizes: time={}, pos={}, sensor={}'.format(time_diff_size, gps_diff_size, sensor_diff_size))
    lat, lon, temp, humid, vcc = stream.readlist('int:24, int:24, int:12, uint:12, uint:8')
    print('first measurement: lat={}, lon={}, temp={}, humid={}, vcc={}'.format(lat, lon, temp, humid, vcc))
    store_data(station_id, now, lat, lon, temp, humid, vcc)

    while True:
        try:
            dtime, dlat, dlon, dtemp, dhumid = stream.readlist('uint:{0}, int:{1}, int:{1}, int:{2}, int:{2}'.format(time_diff_size, gps_diff_size, sensor_diff_size))
        except bitstring.ReadError as e:
            # End of packet reached
            break

        print('next measurement: dtime={}, dlat={}, dlon={}, dtemp={}, dhumid={}'.format(dtime, dlat, dlon, dtemp, dhumid))
        lat -= dlat
        lon -= dlon
        temp -= dtemp
        humid -= dhumid
        # dtime is sent in multiples of 1024ms
        now -= datetime.timedelta(milliseconds = dtime * 1024)
        store_data(station_id, now, lat, lon, temp, humid, None)

def store_data(station_id, time, lat, lon, temp, humid, vcc):
    time_fmt = '%Y-%m-%d %H:%M:%S'

    # Convert fixed-point values to floating point
    lat /= 32768.0
    lon /= 32767.0
    temp /= 16.0
    humid /= 16.0
    if vcc is not None:
        vcc = 1 + vcc / 100.0

    # Check if this measurement isn't already present, by checking for
    # measurements with (nearly) identical timestamps.
    query = """SELECT EXISTS (SELECT 1 FROM `slam_measurement` WHERE 
               `station_id` = %s AND
               `timestamp` > %s AND
               `timestamp` < %s)
            """
    args = (station_id,
            (time - datetime.timedelta(seconds = 3)).strftime(time_fmt),
            (time + datetime.timedelta(seconds = 3)).strftime(time_fmt),
           )

    # Check if the connection is alive, reconnect if needed
    logging.debug("Executing query: {} with args: {}".format(query, args))
    db.ping(True)
    cursor = db.cursor()
    cursor.execute(query, args)
    exists = cursor.fetchone()[0]
    cursor.close()

    if exists:
        print("Measurement already exists, skipping");
        return

    query = """INSERT INTO `slam_measurement` SET 
               `station_id` = %s,
               `timestamp` = %s,
               `latitude` = %s,
               `longitude` = %s,
               `temperature` = %s,
               `humidity` = %s,
               `supply` = %s
            """

    args = (station_id,
            time.strftime(time_fmt),
            lat,
            lon,
            temp,
            humid,
            vcc
           )

    measurement_id = execute_query(db, query, args)

def mqtt_connect(db, app_id=None, access_key=None, ca_cert_path=None, host=None):
    client = mqtt.Client(userdata={'db': db})
    client.on_connect = on_connect
    client.on_message = on_message

    port = 1883

    if app_id is not None and access_key is not None:
        client.username_pw_set(app_id, password=access_key)
    else:
        logging.warn('No App ID or Access key set')

    if ca_cert_path:
        if not os.path.exists(ca_cert_path):
            logging.warning(
                'CA Certificate path specified does not exist, falling back to non-TLS')
        else:
            client.tls_set(ca_cert_path)
            port = 8883

    logging.info('Connecting to {} on port {}'.format(host, port))
    client.connect(host, port=port)
    client.loop_forever()

if __name__ == "__main__":
    app_id = os.environ.get('TTN_APP_ID')
    access_key = os.environ.get('TTN_ACCESS_KEY')
    ttn_host = os.environ.get('TTN_HOST', 'eu.thethings.network')
    ca_cert_path = os.environ.get('TTN_CA_CERT_PATH', 'mqtt-ca.pem')

    mysql_host = os.environ.get('MYSQL_HOST', 'localhost')
    mysql_user = os.environ.get('MYSQL_USER')
    mysql_pwd = os.environ.get('MYSQL_PWD')
    mysql_db = os.environ.get('MYSQL_DB')

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)

    # Open database connection
    db = MySQLdb.connect(mysql_host, mysql_user, mysql_pwd, mysql_db)

    #test_message(db)

    mqtt_connect(db=db, app_id=app_id, access_key=access_key, host=ttn_host, ca_cert_path=ca_cert_path)

# vim: set sw=4 sts=4 expandtab:
