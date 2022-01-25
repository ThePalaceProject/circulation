#!/bin/bash
set -e
source /bd_build/buildconfig
set -x

# Configure nginx.
cp /ls_build/services/syslog-ng.conf /etc/syslog-ng/syslog-ng.conf

