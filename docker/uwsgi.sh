#!/bin/bash
set -ex

# Configure uwsgi.
cp /ls_build/services/uwsgi.ini /etc/uwsgi.ini
cp -R /ls_build/services/uwsgi.d /etc/uwsgi.d
mkdir /var/log/uwsgi
chown -R simplified:simplified /var/log/uwsgi

# Create runit service for uwsgi.
mkdir /etc/service/uwsgi
cp /ls_build/services/uwsgi.runit /etc/service/uwsgi/run
