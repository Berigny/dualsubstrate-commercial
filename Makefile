PROTO_DIR := proto
GEN_PY := api/gen
OPENAPI_OUT := openapi

.PHONY: grpc.gen grpc.openapi grpc.run

grpc.gen:
	python -m grpc_tools.protoc \
	  -I$(PROTO_DIR) \
	  --python_out=$(GEN_PY) \
	  --grpc_python_out=$(GEN_PY) \
	  $(PROTO_DIR)/dualsubstrate/v1/ledger.proto

grpc.run:
	python -m api.grpc_server

# OpenAPI (uses grpc-gateway's openapi plugin via Docker for portability)
grpc.openapi:
	mkdir -p $(OPENAPI_OUT)
	docker run --rm -v $$(pwd):/work -w /work \
	  ghcr.io/grpc-ecosystem/grpc-gateway:latest \
	  protoc -I . \
	    --openapiv2_out=$(OPENAPI_OUT) \
	    --openapiv2_opt=logtostderr=true \
	    $(PROTO_DIR)/dualsubstrate/v1/ledger.proto
	@echo "OpenAPI: $(OPENAPI_OUT)/dualsubstrate.v1.swagger.json"
