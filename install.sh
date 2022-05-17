#!/bin/bash

sudo apt install python3-systemd python3-smbus
pip3 install paho-mqtt
sudo mkdir /usr/local/lib/mqtt2txt
sudo cp mqtt2txt.py /usr/local/lib/mqtt2txt/mqtt2txt.py
sudo cp mqtt2txt.service /etc/systemd/system/mqtt2txt.service
sudo systemctl --system daemon-reload
sudo systemctl enable mqtt2txt.service
sudo systemctl stop mqtt2txt.service
sudo systemctl start mqtt2txt.service
