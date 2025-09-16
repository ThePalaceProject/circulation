.PHONY: opds opds-serve

OPDS_VENV_DIR := tools/opds/.venv
PY := $(OPDS_VENV_DIR)/bin/python
PIP := $(OPDS_VENV_DIR)/bin/pip

$(OPDS_VENV_DIR):
	python3 -m venv $(OPDS_VENV_DIR)

$(PY): $(OPDS_VENV_DIR)
	@true

opds: $(PY)
	$(PIP) install -r tools/opds/requirements.txt
	B2_ENDPOINT=$${B2_ENDPOINT:-s3.us-east-005.backblazeb2.com} \
	B2_KEY_ID=$${B2_KEY_ID:-} \
	B2_APP_KEY=$${B2_APP_KEY:-} \
	B2_BUCKET=$${B2_BUCKET:-} \
	B2_PREFIX=$${B2_PREFIX:-} \
	$(PY) tools/opds/generate_opds2_from_b2.py | cat

opds-serve:
	python3 -m http.server 8080 -d public | cat
