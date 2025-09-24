FROM apache/airflow:2.7.0-python3.9

USER root

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create directory structure
RUN mkdir -p /opt/airflow/storage/bronze \
             /opt/airflow/storage/silver \
             /opt/airflow/storage/gold \
             /opt/airflow/src \
             /opt/airflow/config \
             /opt/airflow/notebooks