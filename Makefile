# Makefile for creating a new release of the package and uploading it to PyPI

PYTHON = python3
CONFIG = sentinelhub.config

help:
	@echo "Use 'make upload' to upload the package to PyPi"

reset-config:
	$(CONFIG) --reset

upload:
	rm -r dist | true
	$(PYTHON) setup.py sdist bdist_wheel
	twine upload --skip-existing dist/*

# For testing:
test-upload:
	rm -r dist | true
	$(PYTHON) setup.py sdist bdist_wheel
	twine upload --repository testpypi --skip-existing dist/*
