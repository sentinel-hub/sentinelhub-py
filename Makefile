# Makefile for creating a new release of the package and uploading it to PyPI
# This way is prefered to manual because it also resets config.json

PYTHON = python3

help:
	@echo "Use 'make upload' to reset config.json and upload the package to PyPi"

upload:
	sentinelhub.config --instance_id "" --aws_access_key_id "" --aws_access_key_id "" --aws_secret_access_key "" --use_s3_l1c_bucket false
	$(PYTHON) setup.py sdist
	twine upload dist/*

# For testing:
test-upload:
	sentinelhub.config --instance_id "" --aws_access_key_id "" --aws_access_key_id "" --aws_secret_access_key "" --use_s3_l1c_bucket false
	$(PYTHON) setup.py sdist
	twine upload --repository testpypi dist/*
