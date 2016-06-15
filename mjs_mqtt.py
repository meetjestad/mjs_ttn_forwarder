import argparse
import base64
import datetime
import json
import logging
import os
import paho.mqtt.client as mqtt
import requests
import ssl
import struct

parser = argparse.ArgumentParser()
parser.add_argument(
    '-d', '--debug', action="store_const", dest="loglevel", const=logging.DEBUG,
    default=logging.WARNING,
)
parser.add_argument(
    '-v', '--verbose', action="store_const", dest="loglevel", const=logging.INFO,
)

TARGET_URL = os.environ.get('MJS_TARGET', 'http://meetjestad.net/beta/add.php')
TARGET_PARAMS = {
    'id': '{dev_eui}',
    'timestamp': '{metadata[0][gateway_time]}',
    'datarate': '{metadata[0][datarate]}',
    'rssi': '{metadata[0][rssi]}',
    'lsnr': '{metadata[0][lsnr]}',
    'lat': '{latitude}',
    'lon': '{longitude}',
    'tmp': '{temperature}',
    'hum': '{humidity}',
}

def on_connect(client, userdata, flags, rc):
    logging.info('Connected to host, subscribing to uplink messages')
    client.subscribe('+/devices/+/up')

def on_message(client, userdata, msg):
    logging.debug('Received message {}'.format(str(msg.payload)))

    try:
        #  Metadata:
        #  {"frequency":868.3,"datarate":"SF7BW125","codingrate":"4/5","gateway_timestamp":3858297787,
        #  "gateway_time":"2016-06-10T08:46:51.138189Z","channel":1,"server_time":"2016-06-10T08:46:48.75436891Z",
        #  "rssi":-35,"lsnr":9.2,"rfchain":1,"crc":1,"modulation":"LORA","gateway_eui":"1DEE0B64B020EEC4",
        #  "altitude":0,"longitude":0,"latitude":0}
        message_payload = json.loads(msg.payload.decode('utf8'))
        payload = base64.b64decode(message_payload.get('payload', ''))

        if len(payload) != 9:
            logging.warn('Invalid packet received with length {}: {}'.format(len(payload), payload))
            return

        # Convert id to integer so backend does not have to do this
        message_payload['dev_eui'] = int(message_payload['dev_eui'], 16)

        # Latitude/Longitude are packed as 3-byte fixed point
        def unpack_coord(data):
            return struct.unpack('>i', b'\x00' + data[:3])[0] / 32768.0

        print(payload)

        message_payload.update({
            'latitude': unpack_coord(payload[:3]),
            'longitude': unpack_coord(payload[3:6]),
            'temperature': (struct.unpack('>h', payload[6:8])[0] >> 4) / 16.0,
            'humidity': (struct.unpack('>h', payload[7:9])[0] & 0xFFF) / 16.0,
        })

        if TARGET_URL != "":
            request_params = {
                k: v.format(**message_payload) for (k, v) in TARGET_PARAMS.items()
            }

            r = requests.get(TARGET_URL, params=request_params)

    except json.JSONDecodeError:
        logging.warn('Received non-JSON message payload')
    except IndexError:
        logging.warn('No metadata on message payload')

def connect(app_eui=None, access_key=None, ca_cert_path=None, host=None):
    client = mqtt.Client()
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

if __name__ == "__main__":
    app_eui = os.environ.get('TTN_APP_EUI')
    access_key = os.environ.get('TTN_ACCESS_KEY')
    ttn_host = os.environ.get('TTN_HOST', 'staging.thethingsnetwork.org')
    ca_cert_path = os.environ.get('TTN_CA_CERT_PATH', 'mqtt-ca.pem')

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)

    connect(app_eui=app_eui, access_key=access_key, host=ttn_host, ca_cert_path=ca_cert_path)
