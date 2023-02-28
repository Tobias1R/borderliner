FROM python:3.11.1-slim

ARG PYTHONDONTWRITEBYTECODE=1
ARG PYTHONBUFFERED=1

RUN apt-get update && apt-get install -y \
    python3-dev gnupg curl \
    unixodbc-dev \
    unixodbc \
    libpq-dev \
    netcat \
    g++ git

# RUN mkdir /opt/borderliner

# COPY . /opt/borderliner

# WORKDIR /opt/borderliner

# RUN python setup.py install

COPY docker/setup.sh /tmp/setup.sh

# Run the bash script
RUN chmod +x /tmp/setup.sh && /tmp/setup.sh

# Cleanup
RUN rm -rf /tmp/setup.sh

CMD ["bash"]
