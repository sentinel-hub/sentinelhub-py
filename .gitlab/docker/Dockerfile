FROM python:3.9

COPY pyproject.toml .
COPY sentinelhub/_version.py sentinelhub/_version.py
COPY LICENSE.md .
COPY README.md .

RUN pip install -e .[AWS,DEV]
