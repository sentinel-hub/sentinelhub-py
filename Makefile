# Makefile for creating a new release of the package and uploading it to PyPI
# Before the release it resets the config.json file

PYTHON = python

help:
	@echo "Use 'make upload' to reset config.json and upload the package to PyPi"

upload:
	sentinelhub.config --instance_id "" --aws_access_key_id "" --aws_access_key_id "" --aws_secret_access_key "" --use_s3_l1c_bucket false
	$(PYTHON) setup.py sdist
	twine upload dist/*
