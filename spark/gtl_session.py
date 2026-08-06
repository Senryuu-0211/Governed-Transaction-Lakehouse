"""Shared SparkSession builder for this project.

The driver runs on the host in a virtualenv, not in a container, so every
dependency is reached through a published port: Kafka on 9093, the Iceberg REST
catalog on 8181. Storage is real AWS S3 (no MinIO container since 2026-07-28),
reached over the AWS endpoint for the configured region. Kafka already advertises
an EXTERNAL listener as localhost:9093 for exactly this kind of host-side client,
so no container network has to be bridged.

Both the streaming job and the smoke test build their session from here, so the
jar versions and catalog wiring can never drift apart between them.
"""

import os
from pathlib import Path

import boto3
from pyspark.sql import SparkSession

# --- Endpoints (host side) --------------------------------------------------
KAFKA_BOOTSTRAP = "localhost:9093"
REST_CATALOG_URI = "http://localhost:8181"
CATALOG = "gtl"
# 28-07: storage = AWS S3 THẬT (bỏ hẳn MinIO). Không còn s3.endpoint / path-style:
# SDK tự dùng endpoint AWS theo region, và S3 thật dùng virtual-host style.
# WAREHOUSE lấy từ .env (S3_BUCKET) để bucket không hard-code trong code.
# Apicurio Schema Registry. Dùng API v2 với ?dereference=true để lấy schema TỰ
# CHỨA: Debezium tách type `...Source` ra artifact riêng (schema references), mà
# Spark from_avro cần schema đã inline đầy đủ. ccompat/latest trả schema chưa
# resolve reference -> from_avro chết "... is not a defined name".
REGISTRY_V2 = "http://localhost:8087/apis/registry/v2"

# --- Dependency versions ----------------------------------------------------
# The two Iceberg artifacts MUST stay on the same version as each other and as
# the apache/iceberg-rest-fixture image in docker-compose.yml. S3 access goes
# through Iceberg's own S3FileIO (shipped in iceberg-aws-bundle) rather than
# hadoop-aws + aws-java-sdk-bundle, which removes the Hadoop/AWS SDK version
# conflict that makes this step fail for most people.
SPARK_VERSION = "3.5.0"
ICEBERG_VERSION = "1.9.2"

PACKAGES = [
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    # Phase 2.5: giải mã Avro (Debezium phát Avro qua Apicurio) trong Bronze stream.
    f"org.apache.spark:spark-avro_2.12:{SPARK_VERSION}",
]

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_env(path: Path = None) -> dict:
    """Read the project .env into a dict.

    Credentials live only in .env (gitignored) so nothing secret ends up in a
    versioned file or in a notebook cell.
    """
    path = path or PROJECT_ROOT / ".env"
    env = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip()
    return env



def s3_client(config=None):
    """Client boto3 + tên bucket — MỘT chỗ duy nhất quyết định endpoint.

    Trước đây ba file tự dựng client riêng (s3_admin, metrics_exporter, maintenance),
    mỗi chỗ lặp lại cùng bốn tham số. Khi thêm chế độ MinIO thì phải sửa cả ba — và
    đó chính là kiểu chỗ dễ sót một cái rồi debug nhầm hướng (đã dính đúng vậy với
    timeout: sửa cho S3FileIO mà quên boto3).

    `S3_ENDPOINT` trong .env quyết định: có giá trị -> MinIO, để trống -> AWS thật.
    """
    env = load_env()
    kwargs = {
        "region_name": env.get("AWS_DEFAULT_REGION", "us-east-1"),
        "aws_access_key_id": env["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": env["AWS_SECRET_ACCESS_KEY"],
    }
    endpoint = env.get("S3_ENDPOINT", "").strip()
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    if config is not None:
        kwargs["config"] = config
    return boto3.client("s3", **kwargs), env["S3_BUCKET"]


def get_spark(
    app_name: str,
    master: str = "local[*]",
    driver_memory: str = "4g",
    extra_conf: dict = None,
) -> SparkSession:
    env = load_env()
    region = env.get("AWS_DEFAULT_REGION", "us-east-1")
    warehouse = f"s3://{env['S3_BUCKET']}/warehouse/"
    # Rỗng/thiếu = AWS S3 thật. Có giá trị = MinIO hoặc store S3-compatible khác.
    endpoint = env.get("S3_ENDPOINT", "").strip()

    # Nạp credential vào MÔI TRƯỜNG, không vào Spark conf (xem lý do ở phần config
    # bên dưới). JVM con kế thừa môi trường này, DefaultCredentialsProvider của AWS
    # SDK đọc đúng ba biến chuẩn dưới đây.
    os.environ.setdefault("AWS_ACCESS_KEY_ID", env["AWS_ACCESS_KEY_ID"])
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", env["AWS_SECRET_ACCESS_KEY"])
    os.environ.setdefault("AWS_REGION", region)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.jars.packages", ",".join(PACKAGES))
        # Iceberg SQL extensions add MERGE INTO, table maintenance calls, and the
        # time-travel syntax the governance phases depend on.
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # --- Catalog: REST + object store nói giao thức S3 --------------------
        # HAI CHẾ ĐỘ, MỘT ĐOẠN CODE. `S3_ENDPOINT` trong .env quyết định:
        #   có giá trị  -> MinIO / bất kỳ store S3-compatible nào (PHẦN 1, $0, localhost)
        #   để trống    -> AWS S3 thật (PHẦN 2, SDK tự resolve endpoint theo region)
        # KHÔNG có nhánh if nào trong job code, không có model dbt nào phải sửa —
        # đó chính là lời hứa "đổi endpoint là lên cloud", và giờ nó kiểm chứng được
        # CẢ HAI CHIỀU (MinIO->S3 ngày 30-07, S3->MinIO ngày 06-08).
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", REST_CATALOG_URI)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        # Endpoint + path-style CHỈ set khi chạy store S3-compatible.
        # MinIO không hỗ trợ virtual-host style (bucket.host) nên bắt buộc path-style
        # (host/bucket); S3 thật thì ngược lại. Đây đúng là hai dòng khác biệt duy
        # nhất giữa hai chế độ.
        .config(f"spark.sql.catalog.{CATALOG}.client.region", region)
        # --- Kết nối HTTP tới S3: BẮT BUỘC cho job dài -----------------------
        # SỰ CỐ 01-08: `maintenance.py` treo ở 0% CPU, nhiều lần, mỗi lần hàng giờ.
        # Không phải chậm — nó ĐANG CHỜ. `ss -tnp` chỉ đúng thủ phạm:
        #     ESTAB  Send-Q=1005  192.168.1.48 -> 52.217.80.92:443   (S3)
        #            ^ đã gửi 1005 byte mà KHÔNG AI ACK
        #
        # ⚠️ HAI NHÓM CẤU HÌNH DƯỚI ĐÂY GIẢI HAI VẤN ĐỀ KHÁC NHAU — tôi đã nhầm
        # một lần: thêm mỗi timeout rồi tưởng xong, nhưng job vẫn treo y hệt.
        #
        # (1) TIMEOUT — cắt khi ĐANG CHỜ TRẢ LỜI.
        #     Cần, nhưng KHÔNG đủ: nó không ngăn client rút ra một kết nối ĐÃ CHẾT
        #     từ connection pool ngay từ đầu.
        .config(f"spark.sql.catalog.{CATALOG}.http-client.type", "apache")
        .config(f"spark.sql.catalog.{CATALOG}.http-client.apache.socket-timeout-ms", "60000")
        .config(f"spark.sql.catalog.{CATALOG}.http-client.apache.connection-timeout-ms", "10000")
        .config(
            f"spark.sql.catalog.{CATALOG}.http-client.apache.connection-acquisition-timeout-ms",
            "30000",
        )
        # (2) TUỔI THỌ KẾT NỐI — mới là thứ sửa đúng gốc.
        #     Máy này ra Internet qua NAT của router gia đình, và NAT nào cũng có
        #     hạn nhàn rỗi: quá hạn thì nó lặng lẽ vứt bản ghi ánh xạ. Hai đầu vẫn
        #     tưởng kết nối còn sống. Lần dùng lại tiếp theo, gói tin bay vào hư
        #     không -> Send-Q kẹt, không bao giờ có ACK, không bao giờ có trả lời.
        #     Job Iceberg đặc biệt dễ dính vì nó thưa thớt: nén một nhóm file mất
        #     nhiều phút không đụng tới S3, thừa thời gian cho NAT quên.
        #     30s idle < mọi hạn NAT thông dụng (thường 60-300s) -> ta chủ động
        #     đóng TRƯỚC khi nó bị vứt, thay vì phát hiện sau khi đã chết.
        .config(
            f"spark.sql.catalog.{CATALOG}.http-client.apache.connection-max-idle-time-ms",
            "30000",
        )
        # Trần cứng cho MỌI kết nối, kể cả đang bận: chặn kiểu hỏng nào mà idle
        # reaper không thấy.
        .config(
            f"spark.sql.catalog.{CATALOG}.http-client.apache.connection-time-to-live-ms",
            "300000",
        )
        # Luồng nền thực sự đi dọn kết nối quá hạn — không có nó thì hai mốc trên
        # chỉ được kiểm lúc tình cờ lấy kết nối ra dùng.
        .config(
            f"spark.sql.catalog.{CATALOG}.http-client.apache.use-idle-connection-reaper-enabled",
            "true",
        )
        # Keepalive TCP: gửi gói thăm dò định kỳ để NAT thấy kết nối vẫn "sống" và
        # không vứt ánh xạ. Bảo hiểm lớp hai cho cùng một vấn đề.
        .config(
            f"spark.sql.catalog.{CATALOG}.http-client.apache.tcp-keep-alive-enabled",
            "true",
        )
        .config("spark.sql.defaultCatalog", CATALOG)
    )
    # Chỉ áp khi có endpoint — để trống là chạy AWS thật.
    if endpoint:
        builder = (
            builder
            .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", endpoint)
            .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        )

    # Job đặc thù (vd push mart qua JDBC cần driver Postgres) truyền conf thêm ở
    # đây thay vì sửa mặc định chung.
    for key, val in (extra_conf or {}).items():
        builder = builder.config(key, val)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
