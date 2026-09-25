"""Tầng semantic: biến một YÊU CẦU ĐO LƯỜNG thành SQL, không phải ngược lại.

VÌ SAO TỒN TẠI
  Agent hỏi-đáp không được sinh SQL tự do. Nó CHỌN một metric và một vài chiều
  đã khai báo sẵn; file này dựng câu lệnh. Khác biệt không nằm ở tiện lợi mà ở
  thứ có thể sai: SQL tự do sai theo vô số cách im lặng (chia trung bình của các
  tỷ lệ, quên lọc chuyển khoản, gộp nhầm grain), còn ở đây những cách đó KHÔNG
  diễn đạt được.

BA THỨ ĐƯỢC THI HÀNH (không phải khuyến nghị)
  1. Tỷ lệ = TỔNG / TỔNG. `type: ratio` cộng riêng hai vế rồi mới chia; không có
     đường nào để viết `avg(tỷ_lệ)`.
  2. Mọi truy vấn TRẢ KÈM WATERMARK (`max(_kafka_offset_max)`) và mốc làm mới.
     Câu trả lời không kèm "tính đến đâu" là câu trả lời không kiểm được.
  3. Chiều và metric phải ĐÃ KHAI BÁO. Tên lạ -> lỗi ngay, không phải SQL hỏng
     ở tận Postgres.

HAI ĐƯỜNG DỮ LIỆU, ĐỪNG TRỘN
  - `filter` trong YAML là của NGƯỜI VIẾT, nhúng thẳng vào SQL (nó là một phần
    của định nghĩa metric).
  - Giá trị lọc từ agent/người dùng đi qua THAM SỐ psycopg (`%s`). Không bao giờ
    nối chuỗi — vừa là chuyện an toàn, vừa là chuyện đúng kiểu dữ liệu.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

METRICS_FILE = Path(__file__).parent / "metrics.yml"

# Trần số dòng trả về. Agent tóm tắt cho người đọc, không ai đọc 50.000 dòng —
# và một truy vấn quên GROUP BY sẽ lộ ra ở đây thay vì làm nghẽn Postgres.
DEFAULT_LIMIT = 1000
MAX_LIMIT = 10_000


class SemanticError(ValueError):
    """Yêu cầu không hợp lệ. Cố tình KHÔNG phải lỗi SQL — bắt ở đây thì thông báo
    còn nói được 'chiều X không tồn tại, các chiều có là...', tức là agent tự sửa
    được. Để rơi xuống Postgres thì chỉ còn một câu syntax error vô dụng."""


@dataclass(frozen=True)
class Source:
    name: str
    table: str
    description: str
    date_column: str
    watermark_column: str
    dimensions: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class Metric:
    name: str
    label: str
    description: str
    source: str
    kind: str                      # "sum" | "ratio"
    fmt: str
    expr: str | None = None        # kind == sum
    numerator: dict[str, str] = field(default_factory=dict)    # kind == ratio
    denominator: dict[str, str] = field(default_factory=dict)


def _measure_sql(expr: str, filter_: str | None) -> str:
    """Một vế của phép đo, đã cộng.

    Có `filter` thì gói vào CASE WHEN chứ KHÔNG đưa lên WHERE: đưa lên WHERE là
    nó lọc luôn cả mẫu số. `success_rate` sẽ thành 100% ở mọi lát cắt — đúng cú
    pháp, sai hoàn toàn về nghĩa, và không có test nào bắt được vì kết quả vẫn là
    một con số trông hợp lý.
    """
    if filter_:
        return f"sum(case when {filter_} then {expr} else 0 end)"
    return f"sum({expr})"


class SemanticLayer:
    def __init__(self, spec: dict[str, Any]):
        self.sources: dict[str, Source] = {
            name: Source(
                name=name,
                table=s["table"],
                description=s.get("description", "").strip(),
                date_column=s["date_column"],
                watermark_column=s["watermark_column"],
                dimensions=s["dimensions"],
            )
            for name, s in spec["sources"].items()
        }
        self.metrics: dict[str, Metric] = {}
        for m in spec["metrics"]:
            if m["source"] not in self.sources:
                raise SemanticError(
                    f"metric '{m['name']}' trỏ tới nguồn không tồn tại: {m['source']}")
            self.metrics[m["name"]] = Metric(
                name=m["name"],
                label=m["label"],
                description=m.get("description", "").strip(),
                source=m["source"],
                kind=m["type"],
                fmt=m.get("format", "decimal"),
                expr=m.get("expr"),
                numerator=m.get("numerator", {}),
                denominator=m.get("denominator", {}),
            )

    @classmethod
    def load(cls, path: Path = METRICS_FILE) -> SemanticLayer:
        return cls(yaml.safe_load(path.read_text(encoding="utf-8")))

    # ---- tra cứu (agent dùng để biết mình có gì) ----------------------------
    def describe(self) -> list[dict[str, Any]]:
        """Danh mục metric kèm chiều dùng được — thứ agent đọc TRƯỚC khi chọn."""
        return [
            {
                "name": m.name,
                "label": m.label,
                "description": m.description,
                "format": m.fmt,
                "dimensions": sorted(self.sources[m.source].dimensions),
            }
            for m in self.metrics.values()
        ]

    # ---- dựng SQL ----------------------------------------------------------
    def build(
        self,
        metrics: str | list[str],
        group_by: list[str] | None = None,
        filters: dict[str, Any] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        order_desc: bool = True,
        limit: int = DEFAULT_LIMIT,
        include_parts: bool = False,
    ) -> tuple[str, list[Any]]:
        """`include_parts`: với metric kiểu tỷ lệ, trả thêm TỬ SỐ và MẪU SỐ đã cộng.

        Cần cho mọi phép tính "phần còn lại": tỷ lệ của nhóm đối chứng KHÔNG phải
        trung bình các tỷ lệ thành viên — nó là (tổng tử − tử của nó) chia
        (tổng mẫu − mẫu của nó). Không có hai vế rời thì phép đó không làm được,
        và người ta sẽ lấy trung bình các tỷ lệ vì đó là thứ duy nhất có trong tay.
        """
        names = [metrics] if isinstance(metrics, str) else list(metrics)
        if not names:
            raise SemanticError("phải chọn ít nhất một metric")

        chosen = []
        for n in names:
            if n not in self.metrics:
                raise SemanticError(
                    f"metric '{n}' không có. Các metric: {', '.join(sorted(self.metrics))}")
            chosen.append(self.metrics[n])

        # Nhiều metric phải CÙNG một nguồn. Ghép chéo hai bảng có grain khác nhau
        # là cách kinh điển để nhân đôi số tiền — nếu cần hai grain thì chạy hai
        # truy vấn rồi ghép ở tầng trên, nơi việc ghép là có ý thức.
        srcs = {m.source for m in chosen}
        if len(srcs) > 1:
            raise SemanticError(
                "các metric phải cùng một nguồn, nhận được: "
                + ", ".join(f"{m.name}->{m.source}" for m in chosen))
        src = self.sources[chosen[0].source]

        group_by = group_by or []
        for d in group_by:
            if d not in src.dimensions:
                raise SemanticError(
                    f"chiều '{d}' không có ở nguồn '{src.name}'. "
                    f"Các chiều: {', '.join(sorted(src.dimensions))}")

        params: list[Any] = []
        where: list[str] = []
        for col, val in (filters or {}).items():
            if col not in src.dimensions:
                raise SemanticError(
                    f"không lọc được theo '{col}' ở nguồn '{src.name}'. "
                    f"Các chiều: {', '.join(sorted(src.dimensions))}")
            if isinstance(val, (list, tuple, set)):
                vals = list(val)
                if not vals:
                    raise SemanticError(f"bộ lọc '{col}' rỗng")
                where.append(f"{col} in ({', '.join(['%s'] * len(vals))})")
                params.extend(vals)
            else:
                where.append(f"{col} = %s")
                params.append(val)

        if date_from:
            where.append(f"{src.date_column} >= %s")
            params.append(date_from)
        if date_to:
            where.append(f"{src.date_column} <= %s")
            params.append(date_to)

        select: list[str] = list(group_by)
        for m in chosen:
            if m.kind == "sum":
                select.append(f"sum({m.expr}) as {m.name}")
            elif m.kind == "ratio":
                num = _measure_sql(m.numerator["expr"], m.numerator.get("filter"))
                den = _measure_sql(m.denominator["expr"], m.denominator.get("filter"))
                # cast numeric: sum(int)/sum(int) ở Postgres là phép chia NGUYÊN,
                # nên một tỷ lệ 0,72 sẽ lặng lẽ thành 0. nullif chặn chia cho 0.
                select.append(
                    f"cast({num} as numeric) / nullif({den}, 0) as {m.name}")
                if include_parts:
                    select.append(f"{num} as {m.name}__num")
                    select.append(f"{den} as {m.name}__den")
            else:
                raise SemanticError(f"metric '{m.name}' có type lạ: {m.kind}")

        # Watermark + độ tươi đi kèm MỌI truy vấn, không phải tuỳ chọn.
        select.append(f"max({src.watermark_column}) as _kafka_offset_max")
        select.append("max(_refreshed_at) as _refreshed_at")

        sql = f"select {', '.join(select)}\nfrom {src.table}"
        if where:
            sql += "\nwhere " + "\n  and ".join(where)
        if group_by:
            sql += "\ngroup by " + ", ".join(group_by)
            # Xếp theo metric ĐẦU TIÊN khi gộp nhóm: câu hỏi xếp hạng ("top 5
            # merchant") chiếm phần lớn lượt dùng, và thứ tự ngẫu nhiên cộng với
            # LIMIT sẽ cho ra một "top 5" sai mà trông vẫn bình thường.
            sql += f"\norder by {chosen[0].name} {'desc' if order_desc else 'asc'} nulls last"
        limit = max(1, min(int(limit), MAX_LIMIT))
        sql += f"\nlimit {limit}"
        return sql, params
