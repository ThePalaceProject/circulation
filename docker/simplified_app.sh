#!/bin/bash
set -e
source /bd_build/buildconfig
set -x

# Add packages we need to build the app and its dependencies
apt-get update
$minimal_apt_get_install --no-upgrade \
  python3 \
  python3-dev \
  python3-setuptools \
  python3-venv \
  python3-pip \
  gcc \
  libpcre3-dev \
  libffi-dev \
  libjpeg-dev \
  libssl-dev \
  libpq-dev \
  libxmlsec1-dev \
  libxmlsec1-openssl \
  pkg-config

# Create a user.
useradd -ms /bin/bash -U simplified


