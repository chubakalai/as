FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY as_updated.py .

# Persistent volume will be mounted here at runtime (see fly.toml)
RUN mkdir -p /data

EXPOSE 8080

CMD ["python", "as_updated.py"]