FROM python:3.11.1-slim

ARG PYTHONDONTWRITEBYTECODE=1
ARG PYTHONBUFFERED=1

RUN apt-get update && apt-get install -y \
    python3-dev gnupg curl \
    unixodbc-dev \
    unixodbc \
    libpq-dev \
    netcat \
    g++

RUN mkdir /opt/borderliner

COPY . /opt/borderliner

WORKDIR /opt/borderliner

RUN python setup.py install

CMD ["bash"]
