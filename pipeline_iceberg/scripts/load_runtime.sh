#!/usr/bin/env bash
# Load a local runtime file when present. Kubernetes injects the same values
# directly through a ConfigMap, so absence of runtime.env is valid in a Pod.
RUNTIME_FILE="${RUNTIME_ENV:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../configs/runtime.env}"
if [[ -f "${RUNTIME_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${RUNTIME_FILE}"
fi
