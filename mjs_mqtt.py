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
    logging.debug('Received message {}'.format(str(msg.payload)))
    db = userdata['db']

    try:
        msg_as_string = msg.payload.decode('utf8')
        now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        message_id = execute_query(db, "INSERT INTO sensors_message SET timestamp = %s, message = %s", (now, msg_as_string))

        message_payload = json.loads(msg_as_string)
        payload = base64.b64decode(message_payload.get('payload_raw', ''))
    # python2 uses ValueError and perhaps others, python3 uses JSONDecodeError
    except Exception as e:
        logging.warn('Error parsing JSON payload')
        logging.warn(e)
        return

    try:
        process_data(db, message_id, message_payload, payload)
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

def process_data(db, message_id, message_payload, payload):
    stream = bitstring.ConstBitStream(bytes=payload)

    port = message_payload["port"]
    if port == 10:
        if len(payload) < 9 or len(payload) > 11:
            logging.warn('Invalid packet received on port {} with length {}'.format(port, len(payload)))
            return
    elif port == 11:
        if len(payload) < 11 or len(payload) > 12:
            logging.warn('Invalid packet received on port {} with length {}'.format(port, len(payload)))
            return
    else:
        logging.warn('Ignoring message with unknown port: {}'.format(port))
        return

    data = {}

    if port == 10:
        data['firmware_version'] = None
    else:
        data['firmware_version'] = stream.read('uint:8')

    data['latitude'] = stream.read('int:24') / 32768.0
    data['longitude'] = stream.read('int:24') / 32768.0
    data['temperature'] = stream.read('int:12') / 16.0
    data['humidity'] = stream.read('int:12') / 16.0

    if len(stream) - stream.bitpos >= 8:
        data['supply'] = 1 + stream.read('uint:8') / 100.0
    else:
        data['supply'] = None

    if len(stream) - stream.bitpos >= 8:
        data['battery'] = 1 + stream.read('uint:8') / 50.0
    else:
        data['battery'] = None

    query = """INSERT INTO `sensors_measurement` SET 
               `station_id` = %s,
               `message_id` = %s,
               `timestamp` = %s,
               `latitude` = %s,
               `longitude` = %s,
               `temperature` = %s,
               `humidity` = %s,
               `battery` = %s,
               `supply` = %s,
               `firmware_version` = %s
            """

    # TODO: Preserve full id?
    station_id = str(int(message_payload['hardware_serial'], 16))
    now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    args = (station_id,
            message_id,
            now,
            data['latitude'],
            data['longitude'],
            data['temperature'],
            data['humidity'],
            data['battery'],
            data['supply'],
            data['firmware_version'],
           )

    measurement_id = execute_query(db, query, args)

    # Record most recent measurement in sensors_station table
    query = """INSERT INTO `sensors_station` (`id`, `last_measurement`, `last_timestamp`) VALUES (%s, %s, %s)
               ON DUPLICATE KEY UPDATE `last_measurement` = %s, `last_timestamp` = %s"""
    args = (station_id, measurement_id, now, measurement_id, now)
    execute_query(db, query, args)

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

def test_message(db):
    msg = mqtt.MQTTMessage()
    msg.payload = """{"app_id":"meet-je-stad","dev_id":"50","hardware_serial":"0000000000000032","port":10,"counter":0,"is_retry":true,"payload_raw":"AAAAAAAAEZP4580=","metadata":{"time":"2017-03-21T10:42:18.464710851Z","frequency":867.1,"modulation":"LORA","data_rate":"SF9BW125","coding_rate":"4/5","gateways":[{"gtw_id":"eui-1dee0b64b020eec4","timestamp":1862821700,"time":"","channel":3,"rssi":-120,"snr":-8.2},{"gtw_id":"eui-1dee1cc11cba7539","timestamp":3425054892,"time":"","channel":3,"rssi":-97,"snr":12.8},{"gtw_id":"eui-1dee18fc1c9d19d8","timestamp":2278897900,"time":"","channel":3,"rssi":-23,"snr":13.5}]}}"""
    on_message(None, {'db': db}, msg)

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
