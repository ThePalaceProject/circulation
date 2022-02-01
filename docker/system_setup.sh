#!/bin/bash
set -e
source /bd_build/buildconfig
set -x

# Make sure base system is up to date
apt-get update && apt-get upgrade -y -o Dpkg::Options::="--force-confold"

# Add packages we need to build the app and its dependencies
install_clean --no-upgrade \
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

# Install Poetry
curl -sSL https://install.python-poetry.org | POETRY_HOME="/opt/poetry" python3 - --yes --version "1.1.12"
ln -s /opt/poetry/bin/poetry /bin/poetry

# Copy scripts that run at startup.
cp /ls_build/startup/* /etc/my_init.d/

# Cleanup
rm -Rf /root/.cache
apt-get clean
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
/bd_build/cleanup.sh
