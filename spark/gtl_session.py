"""Shared SparkSession builder for this project.

The driver runs on the host in a virtualenv, not in a container, so every
dependency is reached through a published port: Kafka on 9093, the Iceberg REST
catalog on 8181, MinIO on 9001. Kafka already advertises an EXTERNAL listener as
localhost:9093 for exactly this kind of host-side client, so no container network
has to be bridged.

Both the streaming job and the smoke test build their session from here, so the
jar versions and catalog wiring can never drift apart between them.
"""

from pathlib import Path

from pyspark.sql import SparkSession

# --- Endpoints (host side) --------------------------------------------------
KAFKA_BOOTSTRAP = "localhost:9093"
REST_CATALOG_URI = "http://localhost:8181"
S3_ENDPOINT = "http://localhost:9001"
WAREHOUSE = "s3://warehouse/"
CATALOG = "gtl"
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


def get_spark(
    app_name: str,
    master: str = "local[*]",
    driver_memory: str = "4g",
    extra_conf: dict = None,
) -> SparkSession:
    env = load_env()
    access_key = env["MINIO_ROOT_USER"]
    secret_key = env["MINIO_ROOT_PASSWORD"]

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
        # --- Catalog: REST now, Glue in the cloud phase -----------------------
        # Only these few lines change to move to AWS Glue + real S3; nothing in
        # the job code refers to MinIO or to a filesystem path.
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", REST_CATALOG_URI)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", S3_ENDPOINT)
        # MinIO serves buckets as a path (endpoint/bucket) rather than as a
        # subdomain, so virtual-host addressing has to be turned off.
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.s3.access-key-id", access_key)
        .config(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key", secret_key)
        # MinIO ignores the region, but the AWS SDK refuses to start without one.
        .config(f"spark.sql.catalog.{CATALOG}.client.region", "us-east-1")
        .config("spark.sql.defaultCatalog", CATALOG)
    )
    # Job đặc thù (vd push mart qua JDBC cần driver Postgres) truyền conf thêm ở
    # đây thay vì sửa mặc định chung.
    for key, val in (extra_conf or {}).items():
        builder = builder.config(key, val)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
