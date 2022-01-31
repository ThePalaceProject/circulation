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

# Pass runtime environment variables to the app at runtime.
touch environment.sh
SIMPLIFIED_ENVIRONMENT=/var/www/circulation/environment.sh
echo "if [[ -f $SIMPLIFIED_ENVIRONMENT ]]; then \
      source $SIMPLIFIED_ENVIRONMENT; fi" >> env/bin/activate

# Update pip and setuptools.
python3 -m pip install -U pip setuptools

# Install Poetry
curl -sSL https://install.python-poetry.org | POETRY_HOME="/opt/poetry" python3 - --yes --version "1.1.12"
ln -s /opt/poetry/bin/poetry /bin/poetry

# Copy scripts that run at startup.
cp /ls_build/startup/* /etc/my_init.d/

# Cleanup
rm -Rf /root/.cache
