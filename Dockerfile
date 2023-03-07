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

# ORACLE SUPPORT
WORKDIR /opt/oracle

RUN curl https://download.oracle.com/otn_software/linux/instantclient/211000/instantclient-basic-linux.x64-21.1.0.0.0.zip \
    --output instantclient-basic-linux.x64-21.1.0.0.0.zip

RUN unzip instantclient-basic-linux.x64-21.1.0.0.0.zip

ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_21_1:$LD_LIBRARY_PATH

RUN apt-get update && apt-get install libaio1 -y

# MSSQL SERVER
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18
RUN apt-get install -y libgssapi-krb5-2

COPY docker/setup.sh /usr/local/bin/setup_borderliner.sh
COPY docker/dockerrun.sh /usr/local/bin/dockerrun.sh 
# Run the bash script
RUN chmod +x /usr/local/bin/setup.sh && /usr/local/bin/setup_borderliner.sh
RUN chmod +x /usr/local/bin/dockerrun.sh 


CMD ["dockerrun.sh"]
