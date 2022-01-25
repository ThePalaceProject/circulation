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

# Change ownership of codebase
chown simplified:simplified /var/www/circulation
cd /var/www/circulation

# Setup virtualenv
python3 -m venv env

# Pass runtime environment variables to the app at runtime.
touch environment.sh
SIMPLIFIED_ENVIRONMENT=/var/www/circulation/environment.sh
echo "if [[ -f $SIMPLIFIED_ENVIRONMENT ]]; then \
      source $SIMPLIFIED_ENVIRONMENT; fi" >> env/bin/activate

# Install Poetry
curl -sSL https://install.python-poetry.org | POETRY_HOME="/opt/poetry" python3 - --yes --version "1.1.12"
ln -s /opt/poetry/bin/poetry /bin/poetry

# Install required python libraries.
set +x && source env/bin/activate && set -x
# Update pip and setuptools.
python3 -m pip install -U pip setuptools
# Install the necessary requirements.
poetry install --no-dev --no-root -E pg
poetry cache clear -n --all pypi

# Install NLTK.
python3 -m textblob.download_corpora lite
mv /root/nltk_data /usr/lib/

# Link the repository code to /home/simplified and change permissions
su - simplified -c "ln -s /var/www/circulation /home/simplified/circulation"
chown -RHh simplified:simplified /home/simplified/circulation

# Give logs a place to go.
mkdir /var/log/simplified

# Copy scripts that run at startup.
cp /ls_build/startup/* /etc/my_init.d/

# Cleanup
rm -Rf /root/.cache
