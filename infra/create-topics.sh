#!/usr/bin/env bash
# Create the pipeline topics. Safe to run more than once.
set -euo pipefail

create() {
  local name=$1 partitions=$2 retention_ms=$3
  if rpk topic describe "$name" >/dev/null 2>&1; then
    echo "topic $name already exists"
  else
    rpk topic create "$name" -p "$partitions" -r 1 -c retention.ms="$retention_ms"
  fi
}

create wiki.edits.raw 3 86400000   # 24 h
create wiki.edits.dlq 1 604800000  # 7 days
rpk topic list
