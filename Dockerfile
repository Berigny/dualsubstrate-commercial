FROM python:3.11-slim

RUN apt-get update && apt-get install -y build-essential portaudio19-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
COPY requirements-fly.txt .
COPY api/requirements.grpc.txt ./requirements.grpc.txt
RUN pip install --no-cache-dir -r requirements-fly.txt

COPY . .

ENV PYTHONUNBUFFERED=1

EXPOSE 8080

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8080"]
