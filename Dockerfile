FROM python:3.11.1-slim

ARG PYTHONDONTWRITEBYTECODE=1
ARG PYTHONBUFFERED=1

RUN apt-get update && apt-get install -y \
    python3-dev gnupg curl \
    unixodbc-dev \
    unixodbc \
    libpq-dev \
    netcat \
    g++ git unzip libxml2 apt-transport-https ca-certificates lsb-release \
    default-libmysqlclient-dev build-essential
# AZURE CLI
# RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# GSUTILS
# RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-cli -y
      

# ORACLE SUPPORT
# WORKDIR /opt/oracle

# RUN curl https://download.oracle.com/otn_software/linux/instantclient/211000/instantclient-basic-linux.x64-21.1.0.0.0.zip \
#     --output instantclient-basic-linux.x64-21.1.0.0.0.zip

# RUN unzip instantclient-basic-linux.x64-21.1.0.0.0.zip

# ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_21_1:$LD_LIBRARY_PATH

# RUN apt-get update && apt-get install libaio1 -y

# MSSQL SERVER 18
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18
RUN apt-get install -y libgssapi-krb5-2

COPY docker/setup.sh /usr/local/bin/setup_borderliner.sh
COPY docker/dockerrun.sh /usr/local/bin/dockerrun.sh 

# IBM DB2 I/Series
COPY docker/drivers/ibm-iaccess-1.1.0.15-1.0.amd64.deb /opt/ibm-iaccess-1.1.0.15-1.0.amd64.deb 
RUN dpkg -i /opt/ibm-iaccess-1.1.0.15-1.0.amd64.deb 
RUN pip install ibm-db==3.1.4 ibm-db-sa==0.3.8

RUN pip install awscli
RUN pip install s3fs==2023.1.0 s3transfer==0.6.0

COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt


# Run the bash script
RUN chmod +x /usr/local/bin/setup_borderliner.sh && /usr/local/bin/setup_borderliner.sh
RUN chmod +x /usr/local/bin/dockerrun.sh 

CMD ["dockerrun.sh"]
