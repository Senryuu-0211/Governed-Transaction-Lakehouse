#!/usr/bin/env bash
# =============================================================================
# Wrapper chạy dbt (session mode) trên host venv.
#   bash scripts/dbt.sh debug | run | test | docs generate ...
#
# Làm 4 việc mà dbt không tự làm được:
#   1. Nạp secrets từ .env -> export AWS_* (S3FileIO đọc qua default credential
#      chain, nhờ vậy spark-defaults.conf + profiles.yml KHÔNG chứa secret).
#   2. RENDER spark-defaults.conf từ .tmpl (Spark không nội suy env var, mà tên
#      bucket S3 không nên nằm trong repo public).
#   3. CÔ LẬP mỗi lần chạy vào thư mục conf + Derby metastore RIÊNG (xem dưới).
#   4. DBT_PROFILES_DIR -> dbt_project/ (profiles.yml nằm cạnh project, không ~/.dbt).
#
# ⚠️ VÌ SAO PHẢI CÔ LẬP (sự cố 01-08):
#   dbt-spark session mode dựng SparkSession ngay trong tiến trình, và Spark boot
#   một Hive metastore Derby nhúng. Derby chỉ cho ĐÚNG MỘT tiến trình mở database:
#     ERROR XSDB6: Another instance of Derby may have already booted the database
#   Chạy hai dbt song song là hỏng. Điều này KHÔNG hiếm: `gtl_transform` (@hourly)
#   và `gtl_maintenance` (@daily, có task reconcile) đều gọi dbt, và cả hai cùng
#   nổ lúc 00:00 UTC. Việc chạy tay trong lúc DAG đang chạy cũng hỏng y hệt.
#   Cách sửa: mỗi lần gọi có derby.system.home riêng trong thư mục tạm -> không
#   tranh nhau. An toàn vì Derby ở đây gần như không dùng: catalog thật là Iceberg
#   REST, Derby chỉ là session catalog mặc định của Spark.
#   (Cùng lý do, SPARK_CONF_DIR cũng phải riêng: hai lần chạy render đè lên nhau.)
# =============================================================================
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

set -a; . "$PROJECT_ROOT/.env"; set +a
# 28-07: AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY nay nằm THẲNG trong .env (S3 thật,
# không còn map từ MINIO_*) và đã được `set -a` export ở trên.
export AWS_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

export DBT_PROFILES_DIR="$PROJECT_ROOT/dbt_project"

# Thư mục tạm riêng cho lần chạy này: chứa conf đã render + Derby metastore.
RUN_DIR="$(mktemp -d -t gtl-dbt-XXXXXX)"
trap 'rm -rf "$RUN_DIR"' EXIT
export SPARK_CONF_DIR="$RUN_DIR/conf"
mkdir -p "$SPARK_CONF_DIR"

# Hai dòng endpoint CHỈ sinh ra khi .env có S3_ENDPOINT (chế độ MinIO). Chạy AWS
# S3 thật thì để trống -> không dòng nào được thêm, SDK tự resolve theo region.
# Cùng một template phục vụ cả hai chế độ, không phải hai file cấu hình song song.
if [ -n "${S3_ENDPOINT:-}" ]; then
  ENDPOINT_CONF="spark.sql.catalog.gtl.s3.endpoint              ${S3_ENDPOINT}
spark.sql.catalog.gtl.s3.path-style-access   true"
else
  ENDPOINT_CONF=""
fi

sed -e "s|__S3_BUCKET__|${S3_BUCKET}|g" \
    -e "s|__AWS_REGION__|${AWS_REGION}|g" \
    -e "s|__DERBY_HOME__|${RUN_DIR}/derby|g" \
    -e "s|__S3_ENDPOINT_CONF__|${ENDPOINT_CONF//$'\n'/\\n}|g" \
    "$PROJECT_ROOT/dbt_project/spark-conf/spark-defaults.conf.tmpl" \
    > "$SPARK_CONF_DIR/spark-defaults.conf"

cd "$PROJECT_ROOT/dbt_project"
exec "$HOME/working/gtl-spark-venv/bin/dbt" "$@"
