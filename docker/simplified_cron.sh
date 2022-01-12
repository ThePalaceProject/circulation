#!/bin/bash

# Switch to local timezone
ln -snf /usr/share/zoneinfo/$TZ /etc/localtime

# Create cron tasks & logfile
cp /ls_build/docker/services/simplified_crontab /etc/cron.d/circulation
touch /var/log/cron.log
