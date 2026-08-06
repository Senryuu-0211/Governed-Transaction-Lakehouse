#!/usr/bin/env python3
"""Quản trị bucket S3 của lakehouse: lifecycle rule + theo dõi dung lượng/chi phí.

    python scripts/s3_admin.py lifecycle   # áp lifecycle rule (idempotent)
    python scripts/s3_admin.py usage       # dung lượng, số object, ước tính $/tháng

VÌ SAO CẦN (bài học 28-07):
  MinIO từng phình 397GB làm đầy ổ. Trên S3 THẬT, cùng vấn đề đó = TIỀN hàng tháng
  + phí request. Script này là lớp phòng thủ ở TẦNG STORAGE, độc lập với Iceberg.

⚠️ LIFECYCLE CHO LAKEHOUSE — CHỖ CỰC DỄ SAI:
  KHÔNG BAO GIỜ đặt rule kiểu "xoá mọi object trong warehouse/ cũ hơn N ngày".
  Data file Iceberg là BẤT BIẾN: một file parquet ghi 1 năm trước vẫn có thể là data
  HIỆN HÀNH của bảng. Xoá theo tuổi = phá nát bảng, mất data vĩnh viễn.
  Vòng đời data file PHẢI do Iceberg quyết (expire_snapshots + remove_orphan_files
  trong spark/maintenance.py) vì chỉ nó biết file nào còn được snapshot tham chiếu.

  Lifecycle ở đây chỉ dọn thứ Iceberg KHÔNG quản:
    1. AbortIncompleteMultipartUpload — phần upload dở của job bị kill giữa chừng.
       Đây là rác VÔ HÌNH: không hiện khi list object nhưng VẪN TÍNH TIỀN. Stream
       của mình từng bị kill rất nhiều lần -> gần như chắc chắn có loại rác này.
    2. Xoá object trong prefix tạm `_smoke/` (chỉ dùng cho test kết nối).
"""

import pathlib
import sys

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "spark"))
from gtl_session import s3_client  # noqa: E402

GB = 1024 ** 3
# Giá S3 Standard us-east-1 (tham khảo, để ước lượng — không phải hoá đơn thật)
USD_PER_GB_MONTH = 0.023




def client_and_bucket():
    """Endpoint do gtl_session quyết định: MinIO (S3_ENDPOINT có) hay AWS thật."""
    return s3_client()


def apply_lifecycle() -> int:
    s3, bucket = client_and_bucket()
    rules = [
        {
            # Rác vô hình tốn tiền: phần multipart upload không bao giờ hoàn tất.
            "ID": "abort-incomplete-multipart-1d",
            "Filter": {"Prefix": ""},
            "Status": "Enabled",
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
        },
        {
            # Prefix tạm cho smoke test — an toàn để xoá theo tuổi.
            "ID": "expire-smoke-test-1d",
            "Filter": {"Prefix": "_smoke/"},
            "Status": "Enabled",
            "Expiration": {"Days": 1},
        },
    ]
    try:
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration={"Rules": rules}
        )
    except s3.exceptions.ClientError as exc:
        if "AccessDenied" not in str(exc):
            raise
        print("❌ IAM user thiếu quyền s3:PutLifecycleConfiguration.")
        print("   Thêm vào policy gtl-s3-access (bucket-level) rồi chạy lại:")
        print('     "s3:PutLifecycleConfiguration", "s3:GetLifecycleConfiguration"')
        return 1
    print(f"✅ đã áp {len(rules)} lifecycle rule lên bucket {bucket}:")
    for rule in rules:
        print(f"   - {rule['ID']}")
    print("\n⚠️  CỐ Ý không có rule xoá theo tuổi cho warehouse/:")
    print("   data file Iceberg bất biến, vòng đời do expire_snapshots +")
    print("   remove_orphan_files (spark/maintenance.py) quyết định.")
    return 0


def show_usage() -> int:
    s3, bucket = client_and_bucket()
    paginator = s3.get_paginator("list_objects_v2")

    total_bytes = total_objects = 0
    by_prefix: dict[str, list[int]] = {}
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            size = obj["Size"]
            total_bytes += size
            total_objects += 1
            # gom theo 3 cấp prefix đầu (vd warehouse/bronze/transactions)
            top = "/".join(obj["Key"].split("/")[:3])
            stat = by_prefix.setdefault(top, [0, 0])
            stat[0] += size
            stat[1] += 1

    print(f"BUCKET {bucket}")
    print(f"  tổng: {total_bytes / GB:.3f} GB · {total_objects:,} object")
    print(f"  ước tính storage: ${total_bytes / GB * USD_PER_GB_MONTH:.4f}/tháng")
    if total_objects:
        print(f"  kích thước TB/object: {total_bytes / total_objects / 1024:.1f} KB "
              f"(quá nhỏ = small-files problem = nhiều PUT = tốn tiền)")
    print("\n  theo prefix:")
    for prefix, (size, count) in sorted(by_prefix.items(), key=lambda x: -x[1][0]):
        print(f"    {prefix:45s} {size / GB:8.3f} GB  {count:>7,} obj")

    # Rác vô hình: multipart upload dở dang KHÔNG hiện trong list_objects.
    try:
        mpu = s3.list_multipart_uploads(Bucket=bucket).get("Uploads", [])
        print(f"\n  multipart upload dở dang: {len(mpu)} "
              f"({'rác vô hình vẫn tính tiền — lifecycle sẽ dọn' if mpu else 'sạch'})")
    except s3.exceptions.ClientError as exc:
        if "AccessDenied" not in str(exc):
            raise
        print("\n  multipart dở dang: (không kiểm được — IAM thiếu "
              "s3:ListBucketMultipartUploads)")
    return 0


def purge_warehouse() -> int:
    """Xoá SẠCH prefix warehouse/ trên S3 — dùng khi reset toàn bộ pipeline.

    ⚠️ Từ 28-07 storage nằm trên S3, nên `docker compose down -v` KHÔNG còn xoá
    data lakehouse nữa (trước đây nó xoá volume MinIO). Không có bước này thì
    reset để lại data mồ côi trên S3 — vừa sai trạng thái, vừa TÍNH TIỀN mãi.
    """
    s3, bucket = client_and_bucket()
    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix="warehouse/"):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if not keys:
            continue
        # delete_objects nhận tối đa 1000 key/lần — cũng chính là cỡ trang.
        s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
        deleted += len(keys)
    print(f"🗑️  đã xoá {deleted:,} object trong s3://{bucket}/warehouse/")
    return 0


def main() -> int:
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "lifecycle":
        return apply_lifecycle()
    if cmd == "usage":
        return show_usage()
    if cmd == "purge":
        return purge_warehouse()
    print(__doc__)
    return 1


if __name__ == "__main__":
    sys.exit(main())
