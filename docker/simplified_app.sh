#!/bin/bash
set -e
source /bd_build/buildconfig
set -x

# Add packages we need to build the app and its dependencies

# We should be able to drop these lines when we move to Python > 3.6
# https://click.palletsprojects.com/en/5.x/python3/#python-3-surrogate-handling
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

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

# Install required python libraries.

# Update pip and setuptools.



# Install NLTK.


# Link the repository code to /home/simplified and change permissions
su - simplified -c "ln -s /var/www/circulation /home/simplified/circulation"
chown -RHh simplified:simplified /home/simplified/circulation

# Give logs a place to go.
mkdir /var/log/simplified

# Copy scripts that run at startup.
cp /ls_build/startup/* /etc/my_init.d/

# Cleanup
rm -Rf /root/.cache
