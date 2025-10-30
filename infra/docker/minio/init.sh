#!/bin/sh
set -euo pipefail
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
echo "Creating $MINIO_DEFAULT_BUCKETS"
for b in $(echo "$MINIO_DEFAULT_BUCKETS" | tr ',' ' '); do
  echo "→ Creating bucket: $b"
  mc mb -p "local/$b" || true
done
mc ls local
mc ls local || true
