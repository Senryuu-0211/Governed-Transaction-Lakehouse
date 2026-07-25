#!/usr/bin/env bash
# =============================================================================
# Wrapper chạy dbt (session mode) trên host venv.
#   bash scripts/dbt.sh debug | run | test | docs generate ...
#
# Làm 3 việc mà dbt không tự làm được:
#   1. Nạp secrets từ .env -> export AWS_* (S3FileIO đọc qua default credential
#      chain, nhờ vậy spark-defaults.conf + profiles.yml KHÔNG chứa secret).
#   2. SPARK_CONF_DIR -> spark-conf/ (jars pin 1.9.2 + Iceberg catalog config).
#   3. DBT_PROFILES_DIR -> dbt_project/ (profiles.yml nằm cạnh project, không ~/.dbt).
# =============================================================================
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

set -a; . "$PROJECT_ROOT/.env"; set +a
export AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER"
export AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD"
export AWS_REGION=us-east-1

export SPARK_CONF_DIR="$PROJECT_ROOT/dbt_project/spark-conf"
export DBT_PROFILES_DIR="$PROJECT_ROOT/dbt_project"

cd "$PROJECT_ROOT/dbt_project"
exec "$HOME/working/gtl-spark-venv/bin/dbt" "$@"
