"""Xuất metric sức khoẻ pipeline ra file .prom cho node_exporter đọc.

VÌ SAO LÀ TEXTFILE, KHÔNG PHẢI MỘT EXPORTER HTTP:
  Cách "chuẩn" là dựng một tiến trình `prometheus_client` mở cổng /metrics 24/7.
  Nhưng ở đây mọi thứ cần đo đều là số ĐO ĐƯỢC BẰNG MỘT CÂU LỆNH RỜI (một câu SQL,
  một lần stat file, một lần gọi boto3) — không có state phải giữ giữa hai lần đo.
  Nuôi thêm một daemon chỉ để chạy 5 câu lệnh mỗi 2 phút là thêm một thứ nữa có
  thể chết lặng lẽ. `node_exporter` ĐÃ chạy 24/7 sẵn và đã được Prometheus scrape;
  textfile collector chỉ là "đọc thêm mấy file .prom trong thư mục này". Ít bộ
  phận chuyển động hơn = ít thứ hỏng hơn.

  Đánh đổi phải biết: metric CŨ tối đa bằng chu kỳ cron (2 phút). Với các rủi ro ta
  canh (WAL tích tụ hàng giờ, stream chết hàng chục phút mới đáng báo động) thì 2
  phút là quá đủ. Nếu sau này cần đo thứ biến thiên theo giây thì mới đổi cách.

TRIẾT LÝ CHỌN METRIC — chỉ đo thứ DẪN TỚI HÀNH ĐỘNG:
  Mỗi metric dưới đây tương ứng một sự cố ĐÃ XẢY RA hoặc một rủi ro đã phân tích
  trong CLAUDE.md. Không có metric nào ở đây chỉ để "cho đẹp dashboard" — metric
  không ai hành động theo thì chỉ là nhiễu, làm loãng thứ thật sự quan trọng.

    gtl_replication_slot_lag_bytes   Rủi ro #1: WAL tích tụ -> ĐẦY ĐĨA DB NGUỒN.
                                     Nguy hiểm hơn mọi thứ khác vì nó giết CORE DB,
                                     không phải giết pipeline.
    gtl_kafka_retention_margin_msgs  Rủi ro #3 (đã xảy ra 20-07): Kafka XOÁ event
                                     trước khi Bronze kịp nuốt. Mất 1,3 triệu event
                                     mà KHÔNG một lỗi nào được ném ra.
    gtl_bronze_freshness_seconds     Sự cố 26-07: stream chết im 10 tiếng. Tôi báo
                                     "đang chạy" vì pgrep thấy tiến trình — dương
                                     tính giả. Bằng chứng thật là TUỔI CHECKPOINT.
    gtl_kafka_lag_messages           Bám kịp nguồn hay đang tụt dần.
    gtl_reconciliation_amount_diff   Governance: tổng tiền Bronze phải = Gold.
    gtl_s3_bytes / gtl_s3_objects    Sự cố 28-07: 397GB rác. Trên S3 thật là TIỀN.

  + `gtl_pipeline_enabled` do `pipeline.sh` ghi (không phải file này) — cờ cổng cho
  MỌI alert. Xem giải thích ở dưới.

CHẠY: mỗi 2 phút qua crontab của user (không cần sudo):
    */2 * * * * cd <project> && PYTHONPATH=spark ~/working/gtl-spark-venv/bin/python \
                scripts/metrics_exporter.py >> ~/gtl-metrics.log 2>&1
"""

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "spark"))
from gtl_session import PROJECT_ROOT, load_env, s3_client  # noqa: E402

OUT_DIR = Path(os.environ.get("GTL_METRICS_DIR", Path.home() / "working" / "metrics"))
OUT_FILE = OUT_DIR / "gtl.prom"

PG_HOST, PG_PORT = "localhost", 5433
KAFKA_CONTAINER = "gtl-kafka"
TOPICS = {  # topic Kafka -> thư mục checkpoint tương ứng của Bronze stream
    "gtl.public.transactions": "transactions",
    "gtl.public.accounts": "accounts",
    "gtl.public.merchants": "merchants",
}

# S3 LIST có tính phí VÀ chậm (phải phân trang toàn bucket). Dung lượng kho thì
# không nhảy theo phút -> đo lại mỗi giờ là thừa đủ để bắt xu hướng phình. Kết quả
# được cache ra file và phát lại nguyên vẹn ở các lần chạy giữa chừng.
S3_REFRESH_SECONDS = 3600
S3_CACHE = OUT_DIR / ".s3_cache.json"

# Cờ "pipeline có ĐANG ĐƯỢC BẬT CÓ CHỦ ĐÍCH không" — do `pipeline.sh` ghi.
# Nó chỉ ghi một file trạng thái nhỏ, còn file .prom vẫn CHỈ MỘT MÌNH exporter này
# ghi: hai tiến trình cùng ghi vào thư mục textfile là công thức cho file đọc dở.
STATE_FILE = OUT_DIR / ".pipeline_state"

# Mọi lệnh gọi ra ngoài đều phải có trần thời gian: cron chạy mỗi 2 phút, một lần
# treo không giới hạn sẽ chồng tiến trình cho tới khi hết RAM.
TIMEOUT = 25


def run(cmd: list) -> str:
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=TIMEOUT, check=True
    ).stdout.strip()


# --- từng nguồn số liệu ------------------------------------------------------
def collect_postgres(env: dict) -> list:
    """Slot lag (rủi ro #1) + lệch đối soát (đọc từ bản sao marts, KHÔNG từ Iceberg).

    Vì sao đối soát lấy ở Postgres: đọc bảng Iceberg cần dựng SparkSession ~40s và
    tốn egress S3 — mỗi 2 phút thì vô lý cả về thời gian lẫn tiền. `push_marts.py`
    đã đẩy `audit_reconciliation` sang Postgres cùng các mart, nên ở đây chỉ còn
    một câu SQL rẻ. Số liệu cũ bằng đúng lần chạy DAG gần nhất — chấp nhận được,
    vì bản thân việc đối soát vốn là công việc theo lô.
    """
    out = []
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=env["POSTGRES_DB"],
        user=env["POSTGRES_USER"], password=env["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )
    try:
        with conn.cursor() as cur:
            # retained_wal = khoảng cách từ vị trí ghi hiện tại tới vị trí slot còn
            # giữ. Đây CHÍNH LÀ lượng WAL Postgres bị cấm xoá vì chờ Debezium.
            cur.execute("""
                select slot_name, active::int,
                       coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)
                from pg_replication_slots
            """)
            for slot, active, retained in cur.fetchall():
                out.append(("gtl_replication_slot_lag_bytes",
                            {"slot": slot}, int(retained)))
                out.append(("gtl_replication_slot_active", {"slot": slot}, active))
    finally:
        conn.close()

    # Đối soát nằm ở DB `marts` khác -> kết nối riêng. Bảng có thể chưa tồn tại
    # (chưa chạy DAG lần nào) -> im lặng bỏ qua, không phải lỗi.
    try:
        mconn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname="marts",
            user=env["POSTGRES_USER"], password=env["POSTGRES_PASSWORD"],
            connect_timeout=10,
        )
        with mconn, mconn.cursor() as cur:
            cur.execute("""
                select coalesce(abs(amount_diff), 0), coalesce(abs(count_diff), 0),
                       extract(epoch from (now() - run_ts))
                from audit_reconciliation order by run_ts desc limit 1
            """)
            row = cur.fetchone()
            if row:
                out.append(("gtl_reconciliation_amount_diff", {}, float(row[0])))
                out.append(("gtl_reconciliation_row_diff", {}, float(row[1])))
                out.append(("gtl_reconciliation_age_seconds", {}, float(row[2])))
        mconn.close()
    except psycopg2.Error:
        pass
    return out


def kafka_offsets(topic: str, time_flag: str) -> int:
    """Offset sớm nhất (-2) hoặc mới nhất (-1) còn tồn tại của topic."""
    raw = run([
        "docker", "exec", KAFKA_CONTAINER, "/opt/kafka/bin/kafka-get-offsets.sh",
        "--bootstrap-server", "localhost:9092", "--topic", topic, "--time", time_flag,
    ])
    # định dạng: topic:partition:offset — cộng dồn mọi partition
    return sum(int(line.rsplit(":", 1)[1]) for line in raw.splitlines() if line)


def checkpoint_offset(name: str) -> int:
    """Offset Bronze stream đã CAM KẾT — đọc từ chính checkpoint của Spark.

    Spark Structured Streaming KHÔNG dùng consumer group của Kafka (nó tự quản
    offset để bảo đảm exactly-once), nên `kafka-consumer-groups.sh` không thấy gì.
    Nguồn sự thật duy nhất là file offset trong checkpoint.
    """
    d = PROJECT_ROOT / "_checkpoints" / name / "offsets"
    files = sorted(int(f.name) for f in d.iterdir() if f.name.isdigit())
    payload = (d / str(files[-1])).read_text().splitlines()[-1]
    return sum(json.loads(payload).get(f"gtl.public.{name}", {}).values())


def retention_margin_ratio(earliest: int, committed: int, latest: int) -> float:
    """Bronze đang đọc tới đâu trong cửa sổ retention: 1.0 = đọc hết, 0.0 = sát mép.

    VÌ SAO PHẢI LÀ TỶ LỆ, KHÔNG PHẢI SỐ MESSAGE (bẫy đã dính 01-08):
      Đặt ngưỡng "cảnh báo khi margin < 50.000 message" nghe rất hợp lý — cho tới
      khi gặp topic `merchants`: cả đời nó chỉ có 50 message và Bronze đã nuốt hết
      sạch, tức là AN TOÀN TUYỆT ĐỐI, nhưng margin=50 nên nó vi phạm ngưỡng VĨNH
      VIỄN. Một ngưỡng tuyệt đối chỉ đúng cho đúng một cỡ topic.
      Tỷ lệ thì đúng cho topic 50 message y như cho topic 3 triệu message.

    Trường hợp cửa sổ rỗng (latest == earliest, topic chưa có message nào) trả 1.0:
    không có gì để mất thì không có gì để cảnh báo.
    """
    window = latest - earliest
    if window <= 0:
        return 1.0
    return round((committed - earliest) / window, 4)


def collect_kafka() -> list:
    out = []
    for topic, name in TOPICS.items():
        latest = kafka_offsets(topic, "-1")
        earliest = kafka_offsets(topic, "-2")
        out.append(("gtl_kafka_end_offset", {"topic": topic}, latest))
        try:
            committed = checkpoint_offset(name)
        except (OSError, ValueError, IndexError, StopIteration):
            continue  # chưa có checkpoint (stream chưa chạy lần nào)
        out.append(("gtl_kafka_lag_messages", {"topic": topic}, latest - committed))
        # Rủi ro #3: khoảng cách từ chỗ đang đọc tới MÉP retention. Số này TIẾN VỀ 0
        # nghĩa là Kafka sắp xoá event mà Bronze chưa nuốt — mất data KHÔNG báo lỗi.
        out.append(("gtl_kafka_retention_margin_msgs",
                    {"topic": topic}, committed - earliest))
        out.append(("gtl_kafka_retention_margin_ratio", {"topic": topic},
                    retention_margin_ratio(earliest, committed, latest)))
    return out


def collect_freshness() -> list:
    """Tuổi lần COMMIT gần nhất của stream — bằng chứng thật là nó còn ghi được.

    Cố ý KHÔNG dùng pgrep: 26-07 tiến trình vẫn còn trong bảng tiến trình suốt 10
    tiếng sau khi query đã chết, và tôi đã báo nhầm "stream đang chạy". Tiến trình
    còn sống không chứng minh được gì; một commit mới thì có.
    """
    out = []
    for name in TOPICS.values():
        d = PROJECT_ROOT / "_checkpoints" / name / "commits"
        try:
            newest = max((f for f in d.iterdir() if f.name.isdigit()),
                         key=lambda f: f.stat().st_mtime)
        except (OSError, ValueError):
            continue
        out.append(("gtl_bronze_freshness_seconds",
                    {"table": name}, int(time.time() - newest.stat().st_mtime)))
    return out


def collect_pipeline_flag() -> list:
    """Pipeline đang được bật CÓ CHỦ ĐÍCH hay đã tắt cố ý — cổng cho mọi alert.

    Đây chính là *maintenance window* làm theo cách rẻ nhất. Không có cờ này thì cứ
    mỗi tối tắt pipeline cho đỡ tốn tiền S3 là sáng hôm sau có một hộp thư đầy cảnh
    báo "stream chết", "lag tăng" — toàn tin đúng nhưng vô dụng. Vài đêm như vậy là
    người ta bắt đầu phớt lờ alert, và lúc có sự cố THẬT cũng phớt lờ nốt. Cảnh báo
    nhiễu còn tệ hơn không có cảnh báo.

    Không đọc được (chưa chạy pipeline.sh lần nào) -> mặc định 1 = ĐANG BẬT. Cố ý
    chọn phía an toàn: thà báo nhầm còn hơn im lặng vì thiếu một file.
    """
    try:
        enabled = 1 if STATE_FILE.read_text().strip() == "1" else 0
    except OSError:
        enabled = 1
    return [("gtl_pipeline_enabled", {}, enabled)]


def collect_s3(env: dict) -> list:
    """Dung lượng + số object của warehouse. Có cache 1 giờ (xem S3_REFRESH_SECONDS)."""
    now = time.time()
    if S3_CACHE.exists() and now - S3_CACHE.stat().st_mtime < S3_REFRESH_SECONDS:
        c = json.loads(S3_CACHE.read_text())
        return [("gtl_s3_bytes", {}, c["bytes"]), ("gtl_s3_objects", {}, c["objects"]),
                ("gtl_s3_measured_age_seconds", {}, int(now - S3_CACHE.stat().st_mtime))]

    s3, _bucket = s3_client()
    total_bytes = total_objects = 0
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=env["S3_BUCKET"], Prefix="warehouse/"
    ):
        for obj in page.get("Contents", []):
            total_bytes += obj["Size"]
            total_objects += 1
    S3_CACHE.write_text(json.dumps({"bytes": total_bytes, "objects": total_objects}))
    return [("gtl_s3_bytes", {}, total_bytes), ("gtl_s3_objects", {}, total_objects),
            ("gtl_s3_measured_age_seconds", {}, 0)]


# --- ghi file ----------------------------------------------------------------
HELP = {
    "gtl_replication_slot_lag_bytes":
        "WAL Postgres bị giữ lại vì slot chưa consume (rủi ro #1: đầy đĩa DB nguồn)",
    "gtl_replication_slot_active": "Slot có consumer đang gắn không (0 = mồ côi, vẫn tích WAL)",
    "gtl_reconciliation_amount_diff":
        "Chênh lệch tuyệt đối tổng tiền Bronze vs Gold ở lần đối soát gần nhất",
    "gtl_reconciliation_row_diff": "Chênh lệch số dòng Bronze vs Gold ở lần đối soát gần nhất",
    "gtl_reconciliation_age_seconds": "Lần đối soát gần nhất cách đây bao lâu",
    "gtl_kafka_end_offset": "Offset mới nhất của topic",
    "gtl_kafka_lag_messages": "Số message Kafka đã có mà Bronze chưa cam kết",
    "gtl_kafka_retention_margin_msgs":
        "Khoảng cách từ offset đang đọc tới mép retention (về 0 = sắp mất data)",
    "gtl_kafka_retention_margin_ratio":
        "Vị trí trong cửa sổ retention: 1=đã nuốt hết, 0=sát mép sắp mất data",
    "gtl_bronze_freshness_seconds": "Tuổi lần commit gần nhất của Bronze stream",
    "gtl_s3_bytes": "Dung lượng warehouse trên S3",
    "gtl_s3_objects": "Số object trong warehouse trên S3",
    "gtl_s3_measured_age_seconds": "Số liệu S3 được đo cách đây bao lâu (cache 1 giờ)",
    "gtl_pipeline_enabled": "Pipeline đang được bật có chủ đích (0 = tắt cố ý, mọi alert im)",
    "gtl_exporter_up": "Exporter chạy trọn vẹn không (0 = có nguồn số liệu lỗi)",
    "gtl_exporter_duration_seconds": "Thời gian chạy exporter",
}


def render(samples: list) -> str:
    lines, seen = [], set()
    for name, labels, value in samples:
        if name not in seen:
            seen.add(name)
            lines.append(f"# HELP {name} {HELP.get(name, name)}")
            lines.append(f"# TYPE {name} gauge")
        lbl = ",".join(f'{k}="{v}"' for k, v in labels.items())
        lines.append(f"{name}{{{lbl}}} {value}" if lbl else f"{name} {value}")
    return "\n".join(lines) + "\n"


def write_atomic(text: str) -> None:
    """Ghi tạm rồi rename — BẮT BUỘC với textfile collector.

    node_exporter có thể đọc thư mục này ĐÚNG LÚC ta đang ghi. Ghi thẳng vào
    gtl.prom sẽ có lúc nó đọc được nửa file -> collector báo lỗi parse và VỨT BỎ
    TOÀN BỘ file. `os.replace` là thao tác nguyên tử trên cùng filesystem: người
    đọc thấy hoặc file cũ trọn vẹn, hoặc file mới trọn vẹn, không bao giờ thấy dở.
    """
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=OUT_DIR, suffix=".tmp")
    with os.fdopen(fd, "w") as f:
        f.write(text)
    os.chmod(tmp, 0o644)  # node_exporter chạy user khác -> phải đọc được
    os.replace(tmp, OUT_FILE)


def main() -> int:
    started = time.time()
    env = load_env()
    samples, failed = [], []

    # Mỗi nguồn được cô lập: Postgres tắt thì vẫn phải xuất được metric S3 và
    # freshness. Một exporter "được ăn cả ngã về không" là exporter mù đúng lúc
    # cần nhìn nhất — lúc có thứ đang hỏng.
    for label, fn in (
        ("postgres", lambda: collect_postgres(env)),
        ("kafka", collect_kafka),
        ("freshness", collect_freshness),
        ("flag", collect_pipeline_flag),
        ("s3", lambda: collect_s3(env)),
    ):
        try:
            samples += fn()
        except Exception as exc:  # noqa: BLE001 — exporter KHÔNG được phép chết
            failed.append(f"{label}: {type(exc).__name__}: {str(exc)[:120]}")

    samples.append(("gtl_exporter_up", {}, 0 if failed else 1))
    samples.append(("gtl_exporter_duration_seconds", {}, round(time.time() - started, 2)))
    write_atomic(render(samples))

    print(f"{time.strftime('%F %T')} {len(samples)} metric -> {OUT_FILE}"
          + (f" | LỖI {'; '.join(failed)}" if failed else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
