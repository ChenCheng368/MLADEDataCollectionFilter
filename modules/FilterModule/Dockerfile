# syntax=docker/dockerfile:1
FROM python:3.7-slim-buster

WORKDIR /app

ENV ANOMALY_DETECTOR_ENDPOINT="${ANOMALY_DETECTOR_ENDPOINT}"
ENV ANOMALY_DETECTOR_KEY="${ANOMALY_DETECTOR_KEY}"

RUN apt-get update && \
    apt-get install -y --no-install-recommends libcurl4-openssl-dev libboost-python-dev libpython3-dev && \
    rm -rf /var/lib/apt/lists/*
RUN pip3 install --upgrade pip
RUN pip3 install setuptools
RUN pip3 install ptvsd==4.1.3
COPY requirements.txt ./
RUN pip3 install -r requirements.txt

COPY . .

RUN useradd -ms /bin/bash moduleuser
USER moduleuser

CMD [ "python3", "-u", "./main.py" ]

