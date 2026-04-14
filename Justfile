# OpenXet — Self-hosted Xet Protocol CAS Server

set dotenv-load := true
set shell := ["bash", "-euo", "pipefail", "-c"]

export RUST_BACKTRACE := "1"
compose := "docker compose -f docker/compose.selfhost.yaml"
compose_local := "docker compose -f docker/compose.local.yaml"
image := "openxet-server"

# List available recipes
default:
    @just --list

# ── Docker (no Rust required) ───────────────────────────────────────────────

# Start the full self-hosted stack (MinIO + XetHub + Litestream + Caddy)
up:
    {{ compose }} up -d --build --force-recreate --remove-orphans

# Start local dev server only (no MinIO, filesystem storage)
dev:
    {{ compose_local }} up -d --build --force-recreate --remove-orphans

# Stop everything
down:
    {{ compose }} down --remove-orphans
    {{ compose_local }} down --remove-orphans 2>/dev/null || true

# Tail logs (optional: just log openxet)
log *svc:
    {{ compose }} logs --tail 100 -f {{ svc }}

# Generate an auth token via Docker (default: write scope, 24h expiry)
token scope="write" repo="*/*":
    @docker run --rm \
        -e OPENXET_AUTH_SECRET="${OPENXET_AUTH_SECRET:-change-me-in-production}" \
        $({{ compose }} config --images 2>/dev/null | head -1 || echo "{{ image }}") \
        generate-token --scope {{ scope }} --repo "{{ repo }}" \
        2>/dev/null || \
    (echo "Image not built yet — building..." && \
        docker build -t {{ image }} -f docker/Dockerfile . && \
        docker run --rm \
            -e OPENXET_AUTH_SECRET="${OPENXET_AUTH_SECRET:-change-me-in-production}" \
            {{ image }} \
            generate-token --scope {{ scope }} --repo "{{ repo }}")

# ── Local development (requires Rust) ───────────────────────────────────────

# Build release binary locally
build-release:
    cargo build --release

# Run all tests locally
test:
    cargo test

# Format code
fmt:
    cargo fmt --all

# Lint code
lint:
    cargo clippy -- -D warnings

# Format + lint + test
check: fmt lint test
