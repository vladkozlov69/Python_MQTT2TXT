#!/bin/bash

sudo cp mqtt2txt.py /usr/local/lib/mqtt2txt/mqtt2txt.py
sudo cp mqtt2txt.service /etc/systemd/system/mqtt2txt.service
sudo systemctl --system daemon-reload
sudo systemctl restart mqtt2txt.service
