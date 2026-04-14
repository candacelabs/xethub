# OpenXet — Self-hosted Xet Protocol CAS Server
# Recipes print commands only — copy/paste to run.

set dotenv-load := true
set shell := ["bash", "-euo", "pipefail", "-c"]

compose := "docker compose -f docker/compose.selfhost.yaml"

# Show how to start the self-hosted stack
up:
    @echo "cargo build --release"
    @echo "{{ compose }} up -d --build --force-recreate --remove-orphans"

# Show how to stop the stack
down:
    @echo "{{ compose }} down --remove-orphans"

# Show how to run all tests
test:
    @echo "RUST_BACKTRACE=1 cargo test"

# Show how to tail logs (pass service name to filter)
log *svc:
    @echo "{{ compose }} logs --tail 100 -f {{ svc }}"

# Show how to generate an auth token
token scope="write" repo="*/*":
    @echo "cargo run -q -p openxet-server -- generate-token --scope {{ scope }} --repo \"{{ repo }}\""

# Show build commands
build-release:
    @echo "cargo build --release"

fmt:
    @echo "cargo fmt --all"

lint:
    @echo "cargo clippy -- -D warnings"

check:
    @echo "cargo fmt --all"
    @echo "cargo clippy -- -D warnings"
    @echo "RUST_BACKTRACE=1 cargo test"
