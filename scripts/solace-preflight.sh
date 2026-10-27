#!/usr/bin/env bash
# Pre-push gate for the feat/solace-connector branch.
#
# Runs, locally, every host-side check in the "Check Solace transport" job of
# .github/workflows/solace-ci.yml, so a push doesn't burn a ~10-minute CI cycle
# on something a laptop catches in seconds. Keep this in lock-step with that job.
#
# Usage:
#   scripts/solace-preflight.sh                 # run all gates
#   ln -s ../../scripts/solace-preflight.sh .git/hooks/pre-push   # or wire as a hook
#
# NOT covered here (by design):
#   - The Bun version / web-console build — only reproducible in the CI image.
#   - The native arm64 container build (the c_char / FFI class). That lives in
#     solace-rs; run its scripts/arch-check-aarch64.sh after changing solace-rs,
#     before bumping the rev here.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

SOLACE_FILES=(
  crates/adapters/src/transport/solace/*.rs
  crates/feldera-types/src/transport/solace.rs
)
WS=(--no-default-features --features with-solace)       # with-solace build
IT=(--no-default-features --features solace-integration-test)  # integration-test build

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$1"; }

step "1/7 rustfmt --check (solace files, edition 2024)"
rustfmt --check --edition 2024 "${SOLACE_FILES[@]}"

step "2/7 cargo check -p dbsp_adapters (with-solace)"
cargo check --locked -p dbsp_adapters "${WS[@]}"

step "3/7 cargo clippy (with-solace, -D warnings)"
cargo clippy --locked -p dbsp_adapters "${WS[@]}" --lib -- -D warnings

step "4/7 cargo check --tests (solace-integration-test)"
cargo check --locked -p dbsp_adapters "${IT[@]}" --tests

step "5/7 cargo test -p feldera-types (config types)"
cargo test --locked -p feldera-types --lib transport::solace

step "6/7 cargo test -p dbsp_adapters (transport::solace)"
cargo test --locked -p dbsp_adapters "${WS[@]}" --lib transport::solace

step "7/7 cargo +stable check -p dbsp_adapters (default features)"
if ! rustup toolchain list 2>/dev/null | grep -q '^stable'; then
  echo "   SKIPPED: no 'stable' toolchain (rustup toolchain install stable)"
elif ! pkg-config --exists rdkafka 2>/dev/null; then
  echo "   SKIPPED: system librdkafka not found — default features need it; CI installs it"
else
  cargo +stable check --locked -p dbsp_adapters
fi

printf '\n\033[1;32mAll host gates passed.\033[0m Arch/FFI (arm64) is verified separately in solace-rs.\n'
