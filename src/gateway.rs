//! HTTP gateway (grpc-gateway + JWT + CORS)
//! Serves REST at :8080, forwards to gRPC :50051

use axum::{
    routing::{get, post, get_service},
    Router, response::Response, http::StatusCode, extract::Request, body::Body,
};
use tower::{ServiceBuilder, ServiceExt};
use tower_http::cors::{Any, CorsLayer};
use hyper::{Client, Uri};
use std::{env, net::SocketAddr, time::Duration};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use once_cell::sync::Lazy;
use serde::Deserialize;

// ---------- JWT ----------
static PUB_KEY: Lazy<Vec<u8>> = Lazy::new(|| {
    std::fs::read(env::var("JWT_PUB_PEM").unwrap_or("/tls/jwt.pub")).unwrap()
});

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
    exp: usize,
}

async fn jwt_layer<B>(req: Request<B>, next: axum::middleware::Next<B>) -> Result<Response, StatusCode> {
    let auth = req.headers()
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    match auth {
        None => Err(StatusCode::UNAUTHORIZED),
        Some(token) => {
            let val = Validation::new(Algorithm::RS256);
            match decode::<Claims>(token, &DecodingKey::from_rsa_pem(&PUB_KEY).unwrap(), &val) {
                Ok(_) => Ok(next.run(req).await),
                Err(_) => Err(StatusCode::UNAUTHORIZED),
            }
        }
    }
}

// ---------- CORS ----------
fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any)
}

// ---------- gRPC-Gateway forward ----------
async fn forward_gateway(mut req: Request<Body>) -> Result<Response, StatusCode> {
    let upstream = env::var("UPSTREAM_GRPC").unwrap_or("http://localhost:50051");
    let uri = format!("{}{}", upstream, req.uri().path_and_query().map(|x| x.as_str()).unwrap_or(""));
    *req.uri_mut() = uri.parse().map_err(|_| StatusCode::BAD_REQUEST)?;

    let client = Client::new();
    let resp = client.request(req).await.map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(resp)
}

// ---------- Axum router ----------
async fn healthz() -> &'static str { "ok" }

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/openapi.json", get(|| async {
            tokio::fs::read_to_string("gen/openapiv2/dualsubstrate.swagger.json").await.unwrap()
        }))
        .route("/docs", get_service(tower_http::services::ServeDir::new("gen/openapiv2"))
            .handle_error(|_| async { "Redoc" }))
        .fallback(forward_gateway)                       // catch-all → gRPC-gateway
        .layer(ServiceBuilder::new()
            .layer(axum::middleware::from_fn(jwt_layer))
            .layer(cors_layer()));

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    println!("Gateway listening on http://{}", addr);
    axum::Server::bind(&addr).serve(app.into_make_service()).await?;
    Ok(())
}
