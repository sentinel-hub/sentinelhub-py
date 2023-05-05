FROM python:3.9

COPY . /sentinelhub-py
RUN cd sentinelhub-py && \
    python -m pip install pip-tools && \
    python -m piptools compile -o requirements.txt pyproject.toml --extra dev --extra aws && \
    python -m pip install -r requirements.txt && \
    python -m pip uninstall pip-tools -y && \
    rm -r /sentinelhub-py
