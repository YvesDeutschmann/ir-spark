#!/usr/bin/env bash
set -euo pipefail

# Idempotent Cloud Agent / local bootstrap: install uv if needed, then sync.
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi

export PATH="${HOME}/.local/bin:${PATH}"
if [[ -f "${HOME}/.local/bin/env" ]]; then
  # shellcheck disable=SC1091
  source "${HOME}/.local/bin/env"
fi

uv sync --frozen
