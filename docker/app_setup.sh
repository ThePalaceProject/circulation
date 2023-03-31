#!/bin/bash
set -e

# Create a user.
useradd -ms /bin/bash -U simplified

cd /var/www/circulation

# Setup & Activate virtualenv
python3 -m venv env
set +x && source env/bin/activate && set -x

# Update pip and setuptools.
python3 -m pip install -U pip setuptools

# Pass runtime environment variables to the app at runtime.
touch environment.sh
SIMPLIFIED_ENVIRONMENT=/var/www/circulation/environment.sh
echo "if [[ -f $SIMPLIFIED_ENVIRONMENT ]]; then \
      source $SIMPLIFIED_ENVIRONMENT; fi" >> env/bin/activate

# Install Python libraries.
poetry install --only main,pg
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
/bd_build/cleanup.sh
