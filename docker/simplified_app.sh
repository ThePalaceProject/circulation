#!/bin/bash
set -e
source /bd_build/buildconfig
set -x

repo="$1"
version="$2"

echo "______________________________________"
echo $version
echo "______________________________________"
if [ -z ${version} ]; then
  echo "WARN: No version specified, will build default branch.";
fi

# Add packages we need to build the app and its dependencies
apt-get update
$minimal_apt_get_install --no-upgrade \
  software-properties-common \
  python3.6 \
  python3-dev \
  python3-setuptools \
  python3-venv \
  python3-pip \
  gcc \
  git \
  libpcre3 \
  libpcre3-dev \
  libffi-dev \
  libjpeg-dev \
  libssl-dev \
  libpq-dev \
  libxmlsec1-dev \
  libxmlsec1-openssl \
  libxml2-dev

# We should be able to drop these lines when we move to Python > 3.6
# https://click.palletsprojects.com/en/5.x/python3/#python-3-surrogate-handling
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

# Create a user.
useradd -ms /bin/bash -U simplified

# Get the proper version of the codebase.
echo "----------"
ls -la
pwd
echo "----------"
#mkdir /var/www && cd /var/www
echo "----------"
ls -la
pwd
echo "----------"
# git clone https://github.com/${repo}.git circulation
# chown simplified:simplified circulation
cd /home/runner/work/circulation/circulation
git checkout $version
echo "----------"
ls -la
pwd
echo "----------"

# Add a .version file to the directory. This file
# supplies an endpoint to check the app's current version.
printf "$(git describe --tags)" > .version

python3 -m venv env

# Pass runtime environment variables to the app at runtime.
touch environment.sh
SIMPLIFIED_ENVIRONMENT=/home/runner/work/circulation/circulation/environment.sh
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
python3 -m textblob.download_corpora
mv /root/nltk_data /usr/lib/

# Link the repository code to /home/simplified and change permissions
su - simplified -c "ln -s /home/runner/work/circulation/circulation /home/simplified/circulation"
chown -RHh simplified:simplified /home/simplified/circulation

# Give logs a place to go.
mkdir /var/log/simplified

# Copy scripts that run at startup.
cp /ls_build/startup/* /etc/my_init.d/
