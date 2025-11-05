FROM python:3.11-slim
RUN apt-get update && apt-get install -y build-essential \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
COPY api/requirements.grpc.txt ./requirements.grpc.txt
RUN pip install --no-cache-dir -r requirements.txt -r requirements.grpc.txt
COPY . .
ENV PYTHONUNBUFFERED=1
EXPOSE 8080 50051
CMD ["python", "-u", "-m", "api.grpc_server"]
