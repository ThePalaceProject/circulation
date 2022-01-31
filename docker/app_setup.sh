#!/bin/bash
set -e

# Create a user.
useradd -ms /bin/bash -U simplified

cd /var/www/circulation

# Setup virtualenv
python3 -m venv env

# Install required python libraries.
set +x && source env/bin/activate && set -x

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

# Cleanup
rm -Rf /root/.cache
