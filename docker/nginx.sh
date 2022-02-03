#!/bin/bash
set -e
source /bd_build/buildconfig
set -x

install_clean nginx

# Configure nginx.
rm /etc/nginx/sites-enabled/default
cp /ls_build/services/nginx.conf /etc/nginx/conf.d/circulation.conf
echo "daemon off;" >> /etc/nginx/nginx.conf

# Prepare nginx for runit.
mkdir /etc/service/nginx
cp /ls_build/services/nginx.runit /etc/service/nginx/run
