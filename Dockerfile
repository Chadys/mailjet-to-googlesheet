FROM python:3.10-buster

ENV PYTHONUNBUFFERED 1

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN pip install --no-cache-dir pip-tools==5.5.0

WORKDIR /code
COPY ./requirements.in ./requirements.in
RUN pip-compile --output-file ./requirements.txt ./requirements.in &&\
    pip-sync ./requirements.txt --pip-args '--no-cache-dir --no-deps'
COPY ./ ./
