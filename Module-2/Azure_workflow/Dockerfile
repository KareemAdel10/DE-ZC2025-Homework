FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
    curl \
    gnupg \
    apt-transport-https \
    g++ \
    unixodbc-dev \
    lsb-release

# Add Microsoft ODBC driver repository
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg
RUN echo "deb [arch=amd64] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list

# Install ODBC driver
RUN apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Copy requirements and install Python dependencies
RUN pip install pyodbc azure-identity
