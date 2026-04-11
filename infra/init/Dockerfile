FROM python:3.11-slim

RUN pip install --no-cache-dir boto3==1.38.0 requests==2.32.3

WORKDIR /app
COPY setup.py .

ENTRYPOINT ["python", "setup.py"]
