#!/usr/bin/python

# place this in /usr/local/lib/mqtt2txt/mqtt2txt.py
# --------------------------------------
# sudo apt install python3-systemd

import paho.mqtt.client as mqttClient
import os
import logging
import signal
import systemd.daemon
from pathlib import Path
from datetime import datetime


files = dict()
files_path = '/home/bmax/mqtt2txt'
closing = False
client = None

off_begin = "08:00:00"
off_end = "10:30:00"
off_begin_t = datetime.strptime(off_begin, "%H:%M:%S")
off_end_t = datetime.strptime(off_end, "%H:%M:%S")

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
        file_name = datetime.now().strftime('%Y-%m-%d_' + self._prefix + '.txt')
        actual_file_name = os.path.join(self._file_path, file_name)
        if (actual_file_name != self._current_file_name):
            self.flush()
            is_new = not Path(actual_file_name).exists()
            self._current_file_name = actual_file_name
            self._file_handle = open(actual_file_name, 'a')
            print('Opened file %s' % actual_file_name)
            return is_new
        return False

    def render_data_internal(self, now, str_data):
        ts = now.strftime('%Y-%m-%d %H:%M:%S,')
        self._file_handle.write(ts + str_data + '\n')

    def render_data(self, str_data):
        global off_begin_t
        global off_end_t
        now = datetime.now()
        off_begin_t = now.replace(hour=off_begin_t.time().hour,
                                  minute=off_begin_t.time().minute,
                                  second=0, microsecond=0)
        off_end_t = now.replace(hour=off_end_t.time().hour,
                                minute=off_end_t.time().minute,
                                second=0, microsecond=0)
        if not (off_begin_t <= now <= off_end_t):
            self.render_data_internal(now, str_data)

    def execute(self, str_data):
        self.prepare_file()
        self.render_data(str_data)

    def flush(self):
        # _LOGGER.debug('Flushing...')
        if (self._file_handle is not None):
            self._file_handle.close()
            self._file_handle = None
            print('Closed file %s' % self._current_file_name)
            self._current_file_name = None


def get_processor_for_topic(topic: str, path: str):
    file_prefix = topic.replace('/', '_')
    if (file_prefix not in files):
        files[file_prefix] = RollingFileProcessor(file_prefix, path)
    return files[file_prefix]


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        global Connected                # Use global variable
        Connected = True                # Signal connection
    else:
        print("Connection failed")


def on_message(client, userdata, message):
    #print("Message received: " + message.topic + " => " + str(message.payload, 'utf-8'))
    if not closing:
        processor = get_processor_for_topic(message.topic, files_path)
        try:
            processor.execute(str(message.payload, 'utf-8'))
        except UnicodeDecodeError:
            print('UnicodeDecodeError occurred')

def close_all():
    global closing
    closing = True
    for value in files.values():
        print('closing')
        value.flush()


def handler_stop_signals(signum, frame):
    if client is not None:
        client.disconnect()


Connected = False   # global variable for the state of the connection


broker_address = "192.168.0.114"  # Broker address
broker_port = 1883               # Broker port

client = mqttClient.Client(clean_session=False, client_id='mqtt2txt')
client.on_connect = on_connect           # attach function to callback
client.on_message = on_message           # attach function to callback
# client.on_disconnect= on_disconnect
client.connect(broker_address, broker_port, 60)    # connect
client.subscribe("I2S/rawRMS")  # subscribe
client.subscribe("I2S/v30000")
client.subscribe("SPH0645/rawRMS")
client.subscribe("SPH0645/v30000")
client.subscribe("3C71BF4822B8/rawRMS")
client.subscribe("3C71BF4822B8/v30000")
client.subscribe("INMP441/rawRMS")
client.subscribe("INMP441/v30000")
client.subscribe("S2_I2S/rawRMS")
client.subscribe("S2_I2S/v30000")
client.subscribe("S3_Test/rawRMS")
client.subscribe("S3_Test/v30000")
client.subscribe("C3Mini/rawRMS")
client.subscribe("C3Mini/v30000")
client.subscribe("ACCEL2/WT31/rawAccX")
client.subscribe("ACCEL2/WT31/rawAccY")
client.subscribe("ACCEL2/WT31/rawAccZ")
client.subscribe("MPU6500/MPU9250/rawAccX")
client.subscribe("MPU6500/MPU9250/rawAccY")
client.subscribe("MPU6500/MPU9250/rawAccZ")
client.subscribe("ACCEL2/ICM20948/rawAccX")
client.subscribe("ACCEL2/ICM20948/rawAccY")
client.subscribe("ACCEL2/ICM20948/rawAccZ")
client.subscribe("ACCEL2/ICM20948/rawGyrX")
client.subscribe("ACCEL2/ICM20948/rawGyrY")
client.subscribe("ACCEL2/ICM20948/rawGyrZ")
client.subscribe("MR60/hrWave")
client.subscribe("MR60/respWave")

signal.signal(signal.SIGTERM, handler_stop_signals)

systemd.daemon.notify('READY=1')

try:
    client.loop_forever()  # then keep listening forever
except KeyboardInterrupt:
    pass
finally:
    close_all()

