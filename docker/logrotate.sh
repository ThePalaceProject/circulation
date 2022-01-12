#!/bin/bash
set -ex

# Add logrotate configuration files
cp /ls_build/docker/services/logrotate.conf /etc/
cp /ls_build/docker/services/default_logrotate /etc/logrotate.d/
cp /ls_build/docker/services/simplified_logrotate.conf /etc/logrotate.d/simplified.conf

chmod 644 /etc/logrotate.conf \
  /etc/logrotate.d/default_logrotate \
  /etc/logrotate.d/simplified.conf

# Remove logrotate for dpkg as we will do
# our own in the default_logrotate.
rm -rf /etc/logrotate.d/dpkg
