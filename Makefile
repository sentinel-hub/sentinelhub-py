# Makefile for creating a new release of the package and uploading it to PyPI
# Before the release it resets the config.json file

help:
	@echo "Use 'make release' to reset config.json and upload the package to PyPi"

release:
	sentinelhub.config --instance_id "" --aws_access_key_id "" --aws_access_key_id "" --aws_secret_access_key "" --use_s3_l1c_bucket false
	python setup.py sdist upload -r pypi
