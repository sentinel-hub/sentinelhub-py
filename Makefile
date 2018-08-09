# Makefile for creating a new release of the package and uploading it to PyPI
# This way is prefered to manual because it also resets config.json

PYTHON = python3
CONFIG = sentinelhub.config

help:
	@echo "Use 'make upload' to reset config.json and upload the package to PyPi"

reset-config:
	$(CONFIG) --instance_id "" \
			  --aws_access_key_id "" \
			  --aws_secret_access_key "" \
			  --ogc_base_url "https://services.sentinel-hub.com/ogc/" \
			  --gpd_base_url "http://service.geopedia.world/" \
			  --aws_metadata_base_url "https://roda.sentinel-hub.com/" \
			  --aws_s3_l1c_bucket "sentinel-s2-l1c" \
			  --aws_s3_l2a_bucket "sentinel-s2-l2a" \
			  --opensearch_url "http://opensearch.sentinel-hub.com/resto/api/collections/Sentinel2/" \
			  --max_wfs_records_per_query 100 \
			  --max_opensearch_records_per_query 500 \
			  --default_start_date "1985-01-01" \
			  --max_download_attempts 4 \
			  --download_sleep_time 5 \
			  --download_timeout_seconds 120


upload: reset-config
	$(PYTHON) setup.py sdist
	twine upload dist/*

# For testing:
test-upload: reset-config
	$(PYTHON) setup.py sdist
	twine upload --repository testpypi dist/*
