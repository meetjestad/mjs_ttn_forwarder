Meet je stad TTN forwarder
==========================

This python script listens to incoming data messages from the The Things
Network, and forwards them to the Meet je stad platform by making an
HTTP request.

To use it, create a "start" script with the following contents (don't
forget to set it as executable):

	#!/bin/sh

	export TTN_APP_EUI=""
	export TTN_ACCESS_KEY=""
	python mjs_mqtt.py "$@"

In this script, fill in the AppEUI and Access key as output by `ttnctl
applications`.

Dependencies
------------
This needs the following dependencies:

	pip install paho-mqtt requests

(Run under sudo to install system-wide)
