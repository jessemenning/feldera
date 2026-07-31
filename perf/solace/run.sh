#!/usr/bin/env bash
# Run the Solace connector performance test.
#
# Usage:
#   ./run.sh [harness args...]     bring up broker+feldera, run the harness
#   ./run.sh down                  tear down the compose stack (and volumes)
#
# See README.md for the full argument reference and run matrix.

set -euo pipefail

cd "$(dirname "$0")"

REQUIRED_MAX_MAP_COUNT=512000

# The Solace broker's internal services die with "Unable to raise event;
# rc(would block)" when vm.max_map_count is too low. On WSL2 the setting must
# land in the WSL2 kernel itself (a privileged container only helps
# Docker-Desktop-style VMs).
ensure_vm_max_map_count() {
    local current
    current=$(sysctl -n vm.max_map_count 2>/dev/null || echo 0)
    if [ "$current" -ge "$REQUIRED_MAX_MAP_COUNT" ]; then
        return 0
    fi
    echo "vm.max_map_count=$current is below $REQUIRED_MAX_MAP_COUNT (required by the Solace broker)."

    if sudo -n sysctl -w "vm.max_map_count=$REQUIRED_MAX_MAP_COUNT" 2>/dev/null; then
        echo "Raised via sudo sysctl."
        return 0
    fi
    if docker run --privileged --rm busybox \
        sysctl -w "vm.max_map_count=$REQUIRED_MAX_MAP_COUNT" 2>/dev/null; then
        # Only effective when the Docker VM shares the host kernel.
        current=$(sysctl -n vm.max_map_count 2>/dev/null || echo 0)
        if [ "$current" -ge "$REQUIRED_MAX_MAP_COUNT" ]; then
            echo "Raised via privileged container."
            return 0
        fi
    fi

    cat >&2 <<EOF
ERROR: could not raise vm.max_map_count.

Fix it persistently (WSL2: run inside the WSL distro, then 'wsl --shutdown'):
  echo 'vm.max_map_count=$REQUIRED_MAX_MAP_COUNT' | sudo tee /etc/sysctl.d/99-solace.conf
  sudo sysctl --system
EOF
    return 1
}

if [ "${1:-}" = "down" ]; then
    docker compose down -v
    exit 0
fi

ensure_vm_max_map_count

echo "Starting Solace broker + Feldera (first broker start can take a few minutes)..."
docker compose up -d --wait

if [ ! -d .venv ]; then
    python3 -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate
pip install --quiet -r requirements.txt
# Use the in-repo Feldera SDK: the published wheel may lag the fork's API.
pip install --quiet -e ../../python

exec python -m solace_perf "$@"
