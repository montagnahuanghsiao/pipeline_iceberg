#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${RUNTIME_ENV:-${SCRIPT_DIR}/../configs/runtime.env}"
python3 "${SCRIPT_DIR}/../jobs/bronze.py" \
  --project-root "${PROJECT_ROOT}" \
  --data-root "$(dirname "${LOCAL_BRONZE_ROOT}")" \
  --start "${START_DATE}" \
  --end "${END_DATE}" \
  --source all
