# syntax=docker/dockerfile:1

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /srv/app

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir fastapi "uvicorn[standard]" pydantic

COPY . /srv/app

EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
