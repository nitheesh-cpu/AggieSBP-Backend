#!/usr/bin/env bash
set -euo pipefail

backend_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cronitor="$(command -v cronitor)"

# Keep Cronitor inside the session so it observes the import's exit code.
# tmux rejects a second session with the same name.
exec tmux new-session -d -s gpa -c "$backend_dir" \
    "$cronitor" exec PpJGqD "$backend_dir/.venv/bin/python" -u \
    "$backend_dir/pipelines/gpa/upsert_gpa_data.py" --report-progress "$@"
