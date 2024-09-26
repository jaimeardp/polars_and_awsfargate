FROM python:3.11-slim

# Set working directory
WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir polars boto3 s3fs python-dotenv pyarrow pandas

# Copy local code to container
COPY . /app

# command to run the Python script
CMD ["python", "process_data_pandas.py"]
