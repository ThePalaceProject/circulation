#!/bin/sh -ex

# Create a key for the CA.
openssl genrsa -out fake_ca.key 2048

# Create a self-signed certificate for the CA.
openssl req \
-x509 \
-new \
-nodes \
-key fake_ca.key \
-days 36500 \
-subj "/C=XX/ST=Somewhere/O=SomewhereCo/CN=CA" \
-out fake_ca.pem

# Request a new server certificate.
openssl req \
-new \
-nodes \
-out fake_server.csr \
-keyout fake_server.key \
-subj "/C=XX/ST=Somewhere/O=SomewhereCo/CN=localhost"

# Sign the server certificate.
openssl x509 \
-req \
-in fake_server.csr \
-CA fake_ca.pem \
-CAkey fake_ca.key \
-CAcreateserial \
-out fake_server.pem \
-days 36500 \
-sha256

# Request a new server certificate.
openssl req \
-new \
-nodes \
-out fake_server_wrong_cn.csr \
-keyout fake_server_wrong_cn.key \
-subj "/C=XX/ST=Somewhere/O=SomewhereCo/CN=wrong"

# Sign the server certificate.
openssl x509 \
-req \
-in fake_server_wrong_cn.csr \
-CA fake_ca.pem \
-CAkey fake_ca.key \
-CAcreateserial \
-out fake_server_wrong_cn.pem \
-days 36500 \
-sha256

# Request a new client certificate.
openssl req \
-new \
-nodes \
-out fake_client.csr \
-keyout fake_client.key \
-subj "/C=XX/ST=Somewhere/O=SomewhereCo/CN=FakeClient0"

# Sign the client certificate.
openssl x509 \
-req \
-in fake_client.csr \
-CA fake_ca.pem \
-CAkey fake_ca.key \
-CAcreateserial \
-out fake_client.pem \
-days 36500 \
-sha256
