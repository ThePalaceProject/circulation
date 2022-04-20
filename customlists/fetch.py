#!/usr/bin/env python

import logging
import argparse
import feedparser
import json
import jsonschema
import os

logging.basicConfig()
logger = logging.getLogger()

parser: argparse.ArgumentParser = argparse.ArgumentParser(
    description="Fetch a custom list."
)
parser.add_argument("--server", help="The address of the CM", required=True)
parser.add_argument("--output", help="The output file", required=True)
parser.add_argument("--list-name", help="The name of the custom list", required=True)
parser.add_argument('--verbose', '-v', action='count', default=0,
                    help="Increase verbosity (can be specified multiple times)")
args: argparse.Namespace = parser.parse_args()

verbose: int = args.verbose or 0
if verbose > 0:
    logger.setLevel(logging.INFO)
if verbose > 1:
    logger.setLevel(logging.DEBUG)

#
# Fetch the list's OPDS feed and parse it.
#

server: str = args.server.rstrip('/')
server_target: str = f'{server}/lists/{args.list_name}/crawlable'
logging.info(f'fetching {server_target}')
feed = feedparser.parse(url_file_stream_or_string=server_target)

#
# Construct a customlist document.
#

document = {}
document['customlists'] = []
document['%id'] = 'https://schemas.thepalaceproject.io/customlists/1.0'

customlists = document['customlists']

customlist = {}
customlist['%type'] = 'customlist'
customlist['books'] = []
customlist['id'] = feed.feed.id
customlist['name'] = feed.feed.title
customlists.append(customlist)

books = customlist['books']

for entry in feed.entries:
    book = {}
    book['%type'] = 'book'
    book['id'] = entry.id
    book['title'] = entry.title
    customlist['books'].append(book)

logger.debug(f'retrieved {len(books)} books')

#
# Validate the customlist document against the schema.
#

with open("customlists.schema.json", "rb") as schema_file:
    schema: str = json.load(schema_file)

jsonschema.validate(document, schema)
logger.debug(f'validated output against schema')

output_file: str = args.output
output_file_tmp: str = output_file + '.tmp'

serialized: str = json.dumps(document, sort_keys=True, indent=2)
with open(output_file_tmp, 'wb') as out:
    out.write(serialized.encode('utf-8'))

os.rename(output_file_tmp, output_file)
