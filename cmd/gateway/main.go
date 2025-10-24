package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	gw "github.com/berigny/dualsubstrate-commercial/gen/go/proto/dualsubstrate/v1"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultListenAddr = ":8080"
	defaultOpenAPI    = "gen/openapiv2/dualsubstrate.swagger.json"
)

func main() {
	upstream := getEnv("UPSTREAM_GRPC", "localhost:50051")
	listenAddr := getEnv("GATEWAY_ADDR", defaultListenAddr)
	openapiPath := getEnv("OPENAPI_PATH", defaultOpenAPI)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	if err := gw.RegisterDualSubstrateHandlerFromEndpoint(ctx, mux, upstream, opts); err != nil {
		log.Fatalf("register dual substrate handler: %v", err)
	}
	if err := gw.RegisterHealthHandlerFromEndpoint(ctx, mux, upstream, opts); err != nil {
		log.Printf("warn: register health handler: %v", err)
	}

	redocHTML := redocPage()

	rootMux := http.NewServeMux()
	rootMux.Handle("/v1/", mux)
	rootMux.Handle("/openapi.json", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := os.ReadFile(openapiPath)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to read OpenAPI: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	rootMux.Handle("/docs", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(redocHTML))
	}))

	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           rootMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("gateway listening on %s -> %s", listenAddr, upstream)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("gateway server error: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func redocPage() string {
	return `<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>DualSubstrate API Docs</title>
    <link rel="icon" href="data:," />
  </head>
  <body>
    <redoc spec-url="/openapi.json"></redoc>
    <script src="https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js"></script>
  </body>
</html>`
}
