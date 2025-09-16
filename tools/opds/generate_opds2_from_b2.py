#!/usr/bin/env python3
import os
import sys
import json
from typing import List, Dict
from urllib.parse import quote

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def ensure_https_endpoint(endpoint: str) -> str:
    endpoint = endpoint.strip()
    if not endpoint:
        return endpoint
    if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
        return "https://" + endpoint
    return endpoint


def build_public_href(endpoint_url: str, bucket: str, key: str) -> str:
    endpoint_url = ensure_https_endpoint(endpoint_url)
    return f"{endpoint_url.rstrip('/')}/{bucket}/{quote(key)}"


def guess_title_from_key(key: str, prefix: str) -> str:
    # Remove prefix if present
    if prefix and key.startswith(prefix):
        name = key[len(prefix):]
    else:
        name = key
    # Drop folders
    name = name.split('/')[-1]
    # Drop extension
    if name.lower().endswith('.epub'):
        name = name[:-5]
    name = name.replace('_', ' ').replace('-', ' ').strip()
    if not name:
        name = key
    return name.title()


def list_epubs(client, bucket: str, prefix: str) -> List[str]:
    keys: List[str] = []
    paginator = client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix or ""):
        contents = page.get('Contents', [])
        for obj in contents:
            key = obj.get('Key')
            if not key:
                continue
            if key.lower().endswith('.epub'):
                keys.append(key)
    return keys


def generate_feed(entries: List[Dict]) -> Dict:
    return {
        "metadata": {"title": "PlayLivros OPDS2"},
        "publications": entries,
    }


def main() -> int:
    endpoint = os.environ.get('B2_ENDPOINT', '').strip()
    key_id = os.environ.get('B2_KEY_ID', '').strip()
    app_key = os.environ.get('B2_APP_KEY', '').strip()
    bucket = os.environ.get('B2_BUCKET', '').strip()
    prefix = os.environ.get('B2_PREFIX', '').strip()

    if not bucket:
        print("B2_BUCKET is required", file=sys.stderr)
        bucket = ""

    endpoint_url = ensure_https_endpoint(endpoint) if endpoint else None

    # Create client; endpoint_url can be None if not provided
    client = boto3.client(
        's3',
        aws_access_key_id=key_id or None,
        aws_secret_access_key=app_key or None,
        endpoint_url=endpoint_url,
    )

    epub_keys: List[str] = []
    try:
        if bucket:
            epub_keys = list_epubs(client, bucket, prefix)
    except (BotoCoreError, ClientError) as e:
        print(f"Warning: failed to list objects from B2: {e}", file=sys.stderr)
        epub_keys = []

    publications: List[Dict] = []
    public_endpoint = endpoint or "https://s3.us-east-005.backblazeb2.com"
    for key in sorted(epub_keys):
        title = guess_title_from_key(key, prefix)
        href = build_public_href(public_endpoint, bucket, key)
        publications.append({
            "metadata": {"title": title},
            "links": [
                {
                    "rel": "http://opds-spec.org/acquisition/open-access",
                    "type": "application/epub+zip",
                    "href": href,
                }
            ],
        })

    feed = generate_feed(publications)

    # Simple validation
    if not isinstance(feed, dict) or "metadata" not in feed or "publications" not in feed:
        print("Generated feed is invalid structure", file=sys.stderr)
        return 2

    out_dir = os.path.join('public', 'feed')
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, 'opds2-playlivros.json')
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)

    # Print only the count to stdout, as requested
    print(len(publications))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
