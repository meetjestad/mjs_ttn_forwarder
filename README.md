Meet je stad TTN forwarder
==========================
This python script listens to incoming data messages from the The Things
Network, and forwards them to the Meet je stad platform by making an
HTTP request.

To use it, create a "start" script with the following contents.

	#!/bin/sh

	export TTN_APP_ID="meet-je-stad"
	export TTN_ACCESS_KEY=""

	export MYSQL_HOST="localhost"
	export MYSQL_USER="meetjestad"
	export MYSQL_PWD=""
	export MYSQL_DB="meetjestad"

	python mjs_mqtt.py "$@"

Fill in the TTN access key and mysql password, and don't forget to set it as
executable.

Dependencies
------------
This needs the following dependencies:

	pip install paho-mqtt requests

(Run under sudo to install system-wide)
