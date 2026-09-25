"""Chạy truy vấn của tầng semantic trên Postgres `marts`.

VÌ SAO KẾT NỐI BẰNG `marts_ro`, KHÔNG PHẢI USER CHÍNH
  Tôn chỉ "agent chỉ chạm marts, không có đường nào tới dữ liệu thô" phải là một
  ràng buộc HẠ TẦNG, không phải một câu dặn trong prompt. `marts_ro` chỉ có
  SELECT trên db `marts`: dù prompt bị lái đi đâu, thứ tệ nhất xảy ra được vẫn
  chỉ là một câu SELECT sai trên bảng tổng hợp.

VÌ SAO TRẢ KÈM WATERMARK
  Mỗi kết quả mang theo `_kafka_offset_max` (tính đến event nào) và
  `_refreshed_at` (job chạy lúc nào). Hai thứ khác nhau: cái đầu truy nguồn
  được, cái sau chỉ là đồng hồ. Câu trả lời cho người không rành kỹ thuật phải
  kèm được "số này tính đến đâu", nếu không thì không ai kiểm lại được.
"""
from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
from layer import DEFAULT_LIMIT, SemanticLayer

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _env(name: str, default: str = "") -> str:
    if name in os.environ:
        return os.environ[name]
    env_file = PROJECT_ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith(f"{name}=") and not line.startswith("#"):
                return line.split("=", 1)[1].strip()
    return default


def dsn() -> str:
    """Chuỗi kết nối tới bản sao phục vụ.

    Dùng `psycopg2` chứ không phải psycopg3 vì host ĐÃ có sẵn nó (metrics_exporter).
    Mang thêm driver thứ hai vào cùng một venv chỉ để dùng API mới hơn là thêm một
    thứ phải nâng cấp và một thứ có thể lệch phiên bản, đổi lấy gần như không gì.
    Nếu sau này cần async thì chuyển CẢ HAI cùng lúc, không chuyển một nửa.
    """
    return (
        f"host={_env('MARTS_HOST', 'localhost')} "
        f"port={_env('MARTS_PORT', '5433')} "
        "user=marts_ro "
        f"password={_env('MARTS_RO_PASSWORD')} "
        "dbname=marts"
    )


def _plain(v: Any) -> Any:
    """Decimal -> float cho JSON. Giữ nguyên kiểu khác (date, bool, None)."""
    return float(v) if isinstance(v, Decimal) else v


def run(
    layer: SemanticLayer,
    metrics: str | list[str],
    group_by: list[str] | None = None,
    filters: dict[str, Any] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    order_desc: bool = True,
    limit: int = DEFAULT_LIMIT,
    include_parts: bool = False,
) -> dict[str, Any]:
    """Trả về {sql, params, columns, rows, watermark, refreshed_at}.

    SQL đi kèm kết quả CÓ CHỦ ĐÍCH: tôn chỉ của agent là "SQL luôn hiện cho
    người dùng xem". Người không rành kỹ thuật sẽ không đọc nó, nhưng người
    phải KÝ vào báo cáo thì có — và đó mới là người cần thuyết phục.
    """
    sql, params = layer.build(
        metrics, group_by=group_by, filters=filters,
        date_from=date_from, date_to=date_to,
        order_desc=order_desc, limit=limit, include_parts=include_parts,
    )
    with psycopg2.connect(dsn()) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        rows = [[_plain(v) for v in r] for r in cur.fetchall()]

    # Với truy vấn có gộp nhóm, watermark của cả tập là GIÁ TRỊ LỚN NHẤT trên mọi
    # dòng, không phải của dòng đầu — max-của-max vẫn đúng khi gộp thêm.
    def _max_of(col: str):
        i = cols.index(col)
        return max((r[i] for r in rows if r[i] is not None), default=None)

    wm = _max_of("_kafka_offset_max")
    ref = _max_of("_refreshed_at")

    keep = [i for i, c in enumerate(cols) if c not in ("_kafka_offset_max", "_refreshed_at")]
    return {
        "sql": sql,
        "params": params,
        "columns": [cols[i] for i in keep],
        "rows": [[r[i] for i in keep] for r in rows],
        "watermark": wm,
        "refreshed_at": ref,
    }
