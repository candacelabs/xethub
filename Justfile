# OpenXet — Self-hosted Xet Protocol CAS Server
# Everything runs in Docker. No Rust, Bun, or other toolchain needed.

set dotenv-load := true
set shell := ["bash", "-euo", "pipefail", "-c"]

export COMPOSE_BAKE := "true"
compose := "docker compose -f docker/compose.selfhost.yaml"
compose_local := "docker compose -f docker/compose.local.yaml"
image := "openxet-server"
dev_image := "openxet-dev"

# List available recipes
default:
    @just --list

# ── Run ─────────────────────────────────────────────────────────────────────

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

# Generate an auth token (default: write scope, 24h expiry)
token scope="write" repo="*/*":
    @just _ensure-image
    @docker run --rm \
        -e OPENXET_AUTH_SECRET="${OPENXET_AUTH_SECRET:-change-me-in-production}" \
        {{ image }} \
        generate-token --scope {{ scope }} --repo "{{ repo }}"

# ── Dev (test / lint / fmt) ─────────────────────────────────────────────────

# Run all tests
test *args: _ensure-dev-image
    docker run --rm -v "$(pwd)":/app -w /app {{ dev_image }} cargo test {{ args }}

# Lint code
lint: _ensure-dev-image
    docker run --rm -v "$(pwd)":/app -w /app {{ dev_image }} cargo clippy -- -D warnings

# Format code
fmt: _ensure-dev-image
    docker run --rm -v "$(pwd)":/app -w /app {{ dev_image }} cargo fmt --all

# Format check (no writes)
fmt-check: _ensure-dev-image
    docker run --rm -v "$(pwd)":/app -w /app {{ dev_image }} cargo fmt --all --check

# Format + lint + test
check: fmt lint test

# ── Internal ────────────────────────────────────────────────────────────────

# Build the runtime image if it doesn't exist
_ensure-image:
    @docker image inspect {{ image }} >/dev/null 2>&1 || \
        (echo "Building {{ image }}..." && docker build -t {{ image }} -f docker/Dockerfile .)

# Build the dev image if it doesn't exist
_ensure-dev-image:
    @docker image inspect {{ dev_image }} >/dev/null 2>&1 || \
        (echo "Building {{ dev_image }}..." && docker build -t {{ dev_image }} -f docker/Dockerfile.dev .)
