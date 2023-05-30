#!/bin/bash
set -ex

# Configure uwsgi.
cp /ls_build/services/uwsgi.ini /etc/uwsgi.ini
cp -R /ls_build/services/uwsgi.d /etc/uwsgi.d
mkdir /var/log/uwsgi
chown -R simplified:simplified /var/log/uwsgi

# Defer uwsgi service to simplified.
mkdir /etc/service/runsvdir-simplified
cp /ls_build/services/simplified_user.runit /etc/service/runsvdir-simplified/run

# Prepare uwsgi for runit.
app_home=/home/simplified
mkdir -p $app_home/service/uwsgi
cp /ls_build/services/uwsgi.runit $app_home/service/uwsgi/run
chown -R simplified:simplified $app_home/service
