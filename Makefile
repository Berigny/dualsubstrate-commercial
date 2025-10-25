PYTHON ?= python3
VENV ?= .venv
VENV_BIN := $(VENV)/bin
PIP := $(VENV_BIN)/pip
PYTHON_BIN := $(VENV_BIN)/python
PYTEST := $(VENV_BIN)/pytest
UVICORN := $(VENV_BIN)/uvicorn

APP_MODULE ?= api.main:app
PYTEST_ARGS ?=

PROTO_DIR := proto
GEN_PY := api/gen
OPENAPI_OUT := openapi

.PHONY: help venv setup run test grpc.gen grpc.openapi grpc.run

help:
	@echo "Common targets:"
	@echo "  make setup    # create venv + install deps for local dev/test"
	@echo "  make run      # start FastAPI app with uvicorn --reload"
	@echo "  make test     # run pytest suite"
	@echo "  make grpc.gen # regenerate Python gRPC stubs"

venv: $(VENV_BIN)/python

$(VENV_BIN)/python:
	$(PYTHON) -m venv $(VENV)

$(VENV)/.installed: requirements.txt $(VENV_BIN)/python
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install pytest
	touch $@

$(VENV)/.grpc-installed: api/requirements.grpc.txt $(VENV)/.installed
	$(PIP) install -r api/requirements.grpc.txt
	touch $@

setup: $(VENV)/.installed

run: setup
	$(UVICORN) $(APP_MODULE) --reload

test: setup
	$(PYTEST) $(PYTEST_ARGS)

grpc.gen: setup $(VENV)/.grpc-installed
	$(PYTHON_BIN) -m grpc_tools.protoc \
	  -I$(PROTO_DIR) \
	  --python_out=$(GEN_PY) \
	  --grpc_python_out=$(GEN_PY) \
	  $(PROTO_DIR)/dualsubstrate/v1/ledger.proto \
	  $(PROTO_DIR)/dualsubstrate/v1/health.proto

grpc.run: $(VENV)/.grpc-installed
	$(PYTHON_BIN) -m api.grpc_server

# OpenAPI (uses grpc-gateway's openapi plugin via Docker for portability)
grpc.openapi:
	mkdir -p $(OPENAPI_OUT)
	docker run --rm -v $$(pwd):/work -w /work \
	  ghcr.io/grpc-ecosystem/grpc-gateway:v2.19.0 \
	  protoc -I . \
	    --openapiv2_out=$(OPENAPI_OUT) \
	    --openapiv2_opt=logtostderr=true \
	    $(PROTO_DIR)/dualsubstrate/v1/ledger.proto
	@echo "OpenAPI: $(OPENAPI_OUT)/dualsubstrate.v1.swagger.json"
