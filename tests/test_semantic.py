"""Test tầng semantic — nhắm vào những cách nó sai IM LẶNG.

Không test "hàm chạy không lỗi". Mỗi test dưới đây gác một cách sai cụ thể mà
kết quả vẫn là một con số trông hợp lý — loại sai không ai phát hiện bằng mắt.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "semantic"))

from layer import SemanticError, SemanticLayer  # noqa: E402


@pytest.fixture(scope="module")
def layer():
    return SemanticLayer.load()


def test_ratio_never_averages_a_ratio(layer):
    """Trung bình của các tỷ lệ là SAI: ngày 3 vụ và ngày 130 vụ không được cân
    bằng nhau. Tỷ lệ đúng là TỔNG chia TỔNG."""
    for name, m in layer.metrics.items():
        if m.kind != "ratio":
            continue
        sql, _ = layer.build(name, group_by=[])
        assert "avg(" not in sql.lower(), f"{name} dùng avg()"
        # Hai vế phải được CỘNG RIÊNG rồi mới chia.
        assert sql.count("sum(") >= 2, f"{name} không cộng đủ hai vế: {sql}"


def test_numerator_filter_does_not_leak_into_where(layer):
    """Bẫy chết người: đưa filter của tử số lên WHERE thì nó lọc CẢ mẫu số ->
    success_rate thành 100% ở mọi lát cắt. Đúng cú pháp, sai hoàn toàn nghĩa,
    và kết quả vẫn là một con số trông bình thường."""
    sql, params = layer.build("success_rate", group_by=["channel"])
    assert "case when status = 'COMPLETED'" in sql
    where = sql.split("where", 1)[1] if "where" in sql else ""
    assert "COMPLETED" not in where, f"filter tử số rò lên WHERE: {where}"
    assert params == []


def test_ratio_casts_to_numeric_before_dividing(layer):
    """Postgres chia hai số NGUYÊN ra số nguyên: một tỷ lệ 0,72 lặng lẽ thành 0.
    Không có ngoại lệ, không có cảnh báo."""
    sql, _ = layer.build("fraud_recall")
    assert "cast(" in sql and "as numeric)" in sql
    assert "nullif(" in sql, "thiếu chặn chia cho 0"


def test_every_query_carries_a_watermark(layer):
    """Câu trả lời không kèm 'tính đến đâu' là câu trả lời không kiểm lại được."""
    for name in layer.metrics:
        sql, _ = layer.build(name)
        assert "_kafka_offset_max" in sql, f"{name} thiếu watermark"
        assert "_refreshed_at" in sql, f"{name} thiếu mốc làm mới"


def test_filter_values_are_parameters_not_string_concatenation(layer):
    """Giá trị lọc đến từ agent/người dùng. Nối chuỗi là vừa mở cửa SQL injection
    vừa hỏng kiểu dữ liệu (ngày tháng, số)."""
    evil = "Central'; drop table mart_txn_daily; --"
    sql, params = layer.build("revenue", filters={"region": evil})
    assert evil not in sql, "giá trị lọc bị nhúng thẳng vào SQL"
    assert params == [evil]
    assert "%s" in sql


def test_unknown_metric_and_dimension_are_rejected_with_a_usable_message(layer):
    """Bắt ở đây thì thông báo còn liệt kê được cái đúng, tức agent tự sửa được.
    Để rơi xuống Postgres thì chỉ còn một câu syntax error vô dụng."""
    with pytest.raises(SemanticError, match="revenue"):
        layer.build("doanh_so_bia_ra")
    with pytest.raises(SemanticError, match="region"):
        layer.build("revenue", group_by=["khong_co_chieu_nay"])
    with pytest.raises(SemanticError, match="region"):
        layer.build("revenue", filters={"khong_co_chieu_nay": 1})


def test_metrics_from_different_sources_cannot_be_mixed(layer):
    """Ghép chéo hai bảng khác grain là cách kinh điển để NHÂN ĐÔI số tiền."""
    with pytest.raises(SemanticError, match="cùng một nguồn"):
        layer.build(["revenue", "active_accounts"])


def test_active_accounts_lives_only_at_day_grain(layer):
    """Đếm-phân-biệt không cộng được: số khách ở hai vùng cộng lại KHÔNG ra số
    khách thật. Đặt nó vào khối rộng là mở đường cho một con số sai."""
    assert layer.metrics["active_accounts"].source == "daily_volume"
    dims = layer.sources["daily_volume"].dimensions
    for d in ("region", "category", "channel", "persona"):
        assert d not in dims, f"daily_volume không được có chiều {d}"


def test_revenue_and_throughput_are_different_metrics(layer):
    """Dùng nhầm throughput làm doanh số thổi con số lên ~15% (LUẬT TIỀN #2)."""
    assert layer.metrics["revenue"].expr == "external_amount"
    assert layer.metrics["throughput"].expr == "net_amount"


def test_limit_is_clamped(layer):
    """Quên GROUP BY hoặc agent xin 10 triệu dòng thì nghẽn Postgres."""
    sql, _ = layer.build("revenue", limit=999_999_999)
    assert "limit 10000" in sql
    sql, _ = layer.build("revenue", limit=0)
    assert "limit 1" in sql


def test_describe_exposes_descriptions_for_the_agent(layer):
    """Agent chọn metric bằng MÔ TẢ. Metric không mô tả = metric agent dùng sai."""
    for d in layer.describe():
        assert d["description"], f"metric {d['name']} không có mô tả"
        assert d["dimensions"], f"metric {d['name']} không có chiều nào"
