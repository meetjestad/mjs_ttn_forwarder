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

SOURCE = 'ttn.v3'

def on_connect(client, userdata, flags, rc):
    logging.info('Connected to host, subscribing to uplink messages')
    client.subscribe('v3/+/devices/+/up')

def on_message(client, userdata, msg):
    logging.debug('Received message {}'.format(str(msg.payload)))
    db = userdata['db']

    try:
        msg_as_string = msg.payload.decode('utf8')
        now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        message_id = execute_query(db, "INSERT INTO sensors_message SET timestamp = %s, message = %s, source = %s", (now, msg_as_string, SOURCE))

        message_payload = json.loads(msg_as_string)
        raw_payload = base64.b64decode(message_payload.get('uplink_message').get('frm_payload', ''))
        port = message_payload["uplink_message"]["f_port"]
        # TODO: Preserve full id?
        station_id = str(int(message_payload["end_device_ids"]["dev_eui"], 16))

    # python2 uses ValueError and perhaps others, python3 uses JSONDecodeError
    except Exception as e:
        logging.warning('Error parsing JSON payload')
        logging.warning(e)
        return

    try:
        process_data(db, message_id, station_id, port, raw_payload)
    except Exception as e:
        logging.warning('Error processing packet')
        logging.warning(e)

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
        logging.warning('Query failed: {}'.format(e))

def process_data(db, message_id, station_id, port, raw_payload):
    stream = bitstring.ConstBitStream(bytes=raw_payload)

    l = len(raw_payload)
    have_supply = False
    have_battery = False
    have_firmware = False
    have_lux = False
    have_pm = False
    have_extra = False
    lux_scale_bits = 0
    if port == 10:
        # Legacy packet without firmware_version, with or without supply
        # and battery
        if l == 9:
            pass
        elif l == 10:
            have_supply = True
        elif l == 11:
            have_supply = True
            have_battery = True
        else:
            logging.warning('Invalid packet received on port {} with length {}'.format(port, l))
            return
    elif port == 11:
        # Packet without lux, with or without 1 byte battery measurement, with
        # or without 4-byte particulate matter
        have_firmware = True
        have_supply = True
        if l == 11:
            pass
        elif l == 12:
            have_battery = True
        elif l == 15:
            have_pm = True
        elif l == 16:
            have_battery = True
            have_pm = True
        else:
            logging.warning('Invalid packet received on port {} with length {}'.format(port, l))
            return
    elif port == 12:
        # Packet with 2-byte lux, with or without 1 byte battery measurement, with or
        # without 4-byte particulate matter
        have_firmware = True
        have_supply = True
        have_lux = True
        if l == 13:
            pass
        elif l == 14:
            have_battery = True
        elif l == 17:
            have_pm = True
        elif l == 18:
            have_battery = True
            have_pm = True
        else:
            logging.warning('Invalid packet received on port {} with length {}'.format(port, l))
            return
    elif port == 13:
        # Packet starting with a flag byte that indicates which of the
        # optional values are present.
        have_firmware = True
        have_supply = True
        have_lux = True
        have_lux = stream.read('bool')
        have_pm = stream.read('bool')
        have_battery = stream.read('bool')
        # 4 bits unused
        stream.read('uint:4')
        have_extra = stream.read('bool')
        # In this packet, the lux is scaled to allow larger values
        lux_scale_bits = 2
    else:
        logging.warning('Ignoring message with unknown port: {}'.format(port))
        return

    data = {}

    if have_firmware:
        data['firmware_version'] = stream.read('uint:8')
    else:
        data['firmware_version'] = None

    data['latitude'] = stream.read('int:24') / 32768.0
    data['longitude'] = stream.read('int:24') / 32768.0
    data['temperature'] = stream.read('int:12') / 16.0
    data['humidity'] = stream.read('int:12') / 16.0

    if have_supply:
        data['supply'] = 1 + stream.read('uint:8') / 100.0
    else:
        data['supply'] = None

    if have_lux:
        data['lux'] = stream.read('uint:16') << lux_scale_bits
    else:
        data['lux'] = None

    if have_pm:
        data['pm2_5'] = stream.read('uint:16')
        data['pm10'] = stream.read('uint:16')
    else:
        data['pm2_5'] = None
        data['pm10'] = None

    if have_battery:
        data['battery'] = 1 + stream.read('uint:8') / 50.0
    else:
        data['battery'] = None

    if have_extra:
        # Extra values are ecoded as pairs of size and value, where size
        # is always 6 bits and the value is size+1 bits long.
        extra_value = ""
        while stream.bitpos < len(stream):
            if len(stream) - stream.bitpos < 5:
                # This can happen due to rounding to whole bytes
                break
            # Add 1 to allow 1-32 bits rather than 0-31
            bits = stream.read('uint:5') + 1
            if len(stream) - stream.bitpos < bits:
                # This can happen due to rounding to whole bytes, in
                # which case the bits should be all-ones
                break
            value = stream.read(bits).uint
            # Just store extra values as a comma-separated string
            if extra_value:
                extra_value += ","
            extra_value += str(value)

        data['extra'] = extra_value
    else:
        data['extra'] = None

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
               `lux` = %s,
               `pm2_5` = %s,
               `pm10` = %s,
               `firmware_version` = %s,
               `extra` = %s
            """

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
            data['lux'],
            data['pm2_5'],
            data['pm10'],
            data['firmware_version'],
            data['extra'],
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
        logging.warning('No App ID or Access key set')

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
    ttn_host = os.environ.get('TTN_HOST', 'eu1.cloud.thethings.network')
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
