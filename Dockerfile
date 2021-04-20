FROM python:3.9-slim

ENV PYTHONUNBUFFERED 1

RUN mkdir -p /code

COPY ./requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt \
    && rm /requirements.txt

COPY ./src /code
