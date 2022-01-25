#!/bin/bash

# Install the necessary requirements.
poetry install --no-dev --no-root -E pg
poetry cache clear -n --all pypi

# Install NLTK.
python3 -m textblob.download_corpora lite
mv /root/nltk_data /usr/lib/

