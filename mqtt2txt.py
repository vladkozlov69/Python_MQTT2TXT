#!/usr/bin/python

# place this in /usr/local/lib/mqtt2txt/mqtt2txt.py
#--------------------------------------
# sudo apt install python3-systemd

import paho.mqtt.client as mqttClient
import time
import os
import logging
import signal
from pathlib import Path
from datetime import datetime


files = dict()
files_path = '/home/vkozlov/'
closing = False

_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)

class RollingFileProcessor:
    _file_path = None
    _prefix = None
    _current_file_name = None
    _file_handle = None

    def __init__(self, prefix, file_path):
        self._prefix = prefix
        self._file_path = file_path

    def prepare_file(self):
        """Checks whether we need to open a handle"""
        file_name = datetime.now().strftime(self._prefix + '%Y%m%d.txt')
        actual_file_name = os.path.join(self._file_path, file_name)
        if (actual_file_name != self._current_file_name):
            self.flush()
            is_new = not Path(actual_file_name).exists()
            self._current_file_name = actual_file_name
            self._file_handle = open(actual_file_name, 'a')
            _LOGGER.debug('Opened file %s' % actual_file_name)
            return is_new
        return False

    def render_data(self, str_data):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S ')
        self._file_handle.write(ts + str_data + '\n')

    def execute(self, str_data):
        self.prepare_file()
        self.render_data(str_data)

    def flush(self):
        _LOGGER.debug('Flushing...')
        if (self._file_handle is not None):
            self._file_handle.close()
            self._file_handle = None
            _LOGGER.debug('Closed file %s' % self._current_file_name)
            self._current_file_name = None



def get_processor_for_topic(topic: str, path: str):
    file_prefix = topic.replace('/', '_')
    if (file_prefix not in files):
        files[file_prefix] = RollingFileProcessor(file_prefix, path)
    return files[file_prefix]


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        global Connected                #Use global variable
        Connected = True                #Signal connection
    else:
        print("Connection failed")


def on_message(client, userdata, message):
    # print("Message received: " + message.topic + " => " + str(message.payload, 'utf-8'))
    if not closing:
        processor = get_processor_for_topic(message.topic, files_path)
        processor.execute(str(message.payload, 'utf-8'))


def close_all():
    closing = True
    for value in files.values():
        print('closing')
        value.flush()


def handler_stop_signals(signum, frame):
    close_all()

Connected = False   #global variable for the state of the connection


broker_address= "192.168.0.114"  #Broker address
broker_port = 1883               #Broker port

client = mqttClient.Client(clean_session=False, client_id='mqtt2txt')    #create new instance
client.on_connect= on_connect                      #attach function to callback
client.on_message= on_message                      #attach function to callback
# client.on_disconnect= on_disconnect
client.connect(broker_address, broker_port, 60)    #connect
client.subscribe("I2S/rawRMS") #subscribe


signal.signal(signal.SIGTERM, handler_stop_signals)

try:
    client.loop_forever() #then keep listening forever
except KeyboardInterrupt:
    pass
finally:
    close_all()
