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
        payload = base64.b64decode(message_payload.get('payload', ''))
    # python2 uses ValueError and perhaps others, python3 uses JSONDecodeError
    except Exception as e:
        logging.warn('Error parsing JSON payload')
        logging.warn(e)

    try:
        if message_payload["port"] == 10:
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

# Latitude/Longitude are packed as 3-byte fixed point
def unpack_coord(data):
    return struct.unpack('>i', b'\x00' + data[:3])[0] / 32768.0

def process_data(db, message_id, message_payload, payload):
    if len(payload) != 9:
        logging.warn('Invalid packet received with length {}: {}'.format(len(payload), payload))
        return

    data = {}
    data['latitude'] = unpack_coord(payload[:3])
    data['longitude'] =unpack_coord(payload[3:6])
    data['temperature'] = (struct.unpack('>h', payload[6:8])[0] >> 4) / 16.0
    data['humidity'] = (struct.unpack('>h', payload[7:9])[0] & 0xFFF) / 16.0

    query = """INSERT INTO `sensors_measurement` SET 
               `station_id` = %s,
               `message_id` = %s,
               `timestamp` = %s,
               `latitude` = %s,
               `longitude` = %s,
               `temperature` = %s,
               `humidity` = %s
            """

    args = (int(message_payload['dev_eui'], 16),
            message_id,
            time.strftime('%Y-%m-%d %H:%M:%S'),
            data['latitude'],
            data['longitude'],
            data['temperature'],
            data['humidity'],
           )

    execute_query(db, query, args)

def mqtt_connect(db, app_eui=None, access_key=None, ca_cert_path=None, host=None):
    client = mqtt.Client(userdata={'db': db})
    client.on_connect = on_connect
    client.on_message = on_message

    port = 1883

    if app_eui is not None and access_key is not None:
        client.username_pw_set(app_eui, password=access_key)
    else:
        logging.warn('No App EUI or Access key set')

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
    msg.payload = """{"payload":"AAAAAAAAAFXU","port":10,"counter":20,"dev_eui":"0000000000000016","metadata":[{"frequency":868.1,"datarate":"SF9BW125","codingrate":"4/5","gateway_timestamp":2053707724,"channel":0,"server_time":"2016-11-26T18:09:17.938315364Z","rssi":-120,"lsnr":-10.2,"rfchain":1,"crc":1,"modulation":"LORA","gateway_eui":"1DEE0B64B020EEC4","altitude":0,"longitude":5.37687,"latitude":52.16273}]}"""
    on_message(None, {'db': db}, msg)

if __name__ == "__main__":
    app_eui = os.environ.get('TTN_APP_EUI')
    access_key = os.environ.get('TTN_ACCESS_KEY')
    ttn_host = os.environ.get('TTN_HOST', 'staging.thethingsnetwork.org')
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

    mqtt_connect(db=db, app_eui=app_eui, access_key=access_key, host=ttn_host, ca_cert_path=ca_cert_path)

