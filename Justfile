# OpenXet — Self-hosted Xet Protocol CAS Server

set dotenv-load := true
set shell := ["bash", "-euo", "pipefail", "-c"]

export RUST_BACKTRACE := "1"
compose := "docker compose -f docker/compose.selfhost.yaml"

# Start everything (build + deploy self-hosted stack)
up: build-release
    {{ compose }} up -d --build --force-recreate --remove-orphans
    @echo ""
    @echo "Stack running. Generate a token:"
    @echo "  just token"

# Stop everything
down:
    {{ compose }} down --remove-orphans

# Run all tests
test:
    cargo test

# Tail logs (optional: just log openxet)
log *svc:
    {{ compose }} logs --tail 100 -f {{ svc }}

# Generate an auth token (default: write scope, 24h expiry)
token scope="write" repo="*/*":
    cargo run -q -p openxet-server -- generate-token --scope {{ scope }} --repo "{{ repo }}"

# ── Build ─────────────────────────────────────────────────────────────────────

build-release:
    cargo build --release

fmt:
    cargo fmt --all

lint:
    cargo clippy -- -D warnings

check: fmt lint test
