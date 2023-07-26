#!/usr/local/bin/python3.11

# place this in /usr/local/lib/mqtt2txt/mqtt2txt.py
# --------------------------------------
# sudo apt install python3-systemd

import paho.mqtt.client as mqttClient
import os
import logging
import signal
import cysystemd.daemon
from pathlib import Path
from datetime import datetime


files = dict()
files_path = '/home/vkozlov/mqtt2txt'
closing = False
client = None

off_begin = "11:00:00"
off_end = "12:00:00"
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


broker_address = "127.0.0.1"  # Broker address
broker_port = 1883               # Broker port

client = mqttClient.Client(clean_session=False, client_id='mqtt2txt')
client.on_connect = on_connect           # attach function to callback
client.on_message = on_message           # attach function to callback
# client.on_disconnect= on_disconnect
client.connect(broker_address, broker_port, 60)    # connect


#client.subscribe("C3_Test1/radar/csv")
#client.subscribe("C3_Test1/unknown")
#client.subscribe("C3_Test1/raw/hrWave")
#client.subscribe("C3_Test1/raw/respWave")

#client.subscribe("esp32snore/v30000")


client.subscribe("RtlADXL/ADXL35X/rawAccX")
client.subscribe("RtlADXL/ADXL35X/rawAccY")
client.subscribe("RtlADXL/ADXL35X/rawAccZ")
client.subscribe("RtlADXL/ADXL35X/csv")
client.subscribe("RtlADXL/ADXL35X/breath")

client.subscribe("EspADXL/ADXL35X/rawAccX")
client.subscribe("EspADXL/ADXL35X/rawAccY")
client.subscribe("EspADXL/ADXL35X/rawAccZ")
client.subscribe("EspADXL/ADXL35X/csv")
client.subscribe("EspADXL/ADXL35X/breath")

#client.subscribe("VL53L5CX/VL53L5CX/raw")

client.subscribe("SLEEP4/VL53L5CX/raw")
client.subscribe("SLEEP4/v30000")
client.subscribe("SLEEP4/radar/csv")
#client.subscribe("SLEEP4/raw/respWave")
#client.subscribe("SLEEP4/raw/hrWave")

client.subscribe("Zero/VL53L5CX/raw")
client.subscribe("Zero/v30000")
client.subscribe("Zero/radar/csv")

client.subscribe("7C9EBD4974B4/ADS1115/value")
client.subscribe("7C9EBD4974B4/ADS1115/csv")
client.subscribe("7C9EBD4974B4/ADS1115/brRaw")
client.subscribe("7C9EBD4974B4/ADS1115/hrRaw")
client.subscribe("7C9EBD4974B4/ADS1115/brRaw2")
client.subscribe("7C9EBD4974B4/ADS1115/hrRaw2")

client.subscribe("RESMED/ADS1115/value0")
client.subscribe("RESMED/ADS1115/value1")
client.subscribe("RESMED/ADS1115/value2")
client.subscribe("RESMED/ADS1115/value3")

client.subscribe("RESMED/ADS1115/max0")
client.subscribe("RESMED/ADS1115/max1")
client.subscribe("RESMED/ADS1115/max2")
client.subscribe("RESMED/ADS1115/max3")

client.subscribe("RESMED/ADS1115/stddev0")
client.subscribe("RESMED/ADS1115/stddev1")
client.subscribe("RESMED/ADS1115/stddev2")
client.subscribe("RESMED/ADS1115/stddev3")

signal.signal(signal.SIGTERM, handler_stop_signals)

cysystemd.daemon.notify(cysystemd.daemon.Notification.READY)

try:
    client.loop_forever()  # then keep listening forever
except KeyboardInterrupt:
    pass
finally:
    close_all()
    
