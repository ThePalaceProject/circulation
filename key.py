import json
from pathlib import Path

from jwcrypto import jwk

# Load from a JWK dict
private_jwk = json.loads(Path("~/Desktop/demarque.key").expanduser().read_text())

key = jwk.JWK(**private_jwk)

# Export as PEM
pem = key.export_to_pem(private_key=True, password=None)
print(pem.decode("utf-8"))
