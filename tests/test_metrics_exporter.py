"""Unit test cho logic thuần của exporter metric.

Ở đây CỐ Ý không test phần gọi Postgres/Kafka/S3: những thứ đó cần hệ thống thật
và đã được `verify_5.sh` kiểm end-to-end. Cái đáng test bằng unit test là phần
LOGIC — nơi một dấu lớn-bé sai không làm chương trình chết, chỉ làm nó nói dối.

Mọi test dưới đây đều bắt nguồn từ một lỗi CÓ THẬT hoặc một biên đã suýt sai.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "spark"))

from metrics_exporter import render, retention_margin_ratio  # noqa: E402


class TestRetentionMarginRatio:
    """Bẫy đã dính 01-08: ngưỡng tuyệt đối trên thứ có ý nghĩa tương đối."""

    def test_da_nuot_het_thi_an_toan_tuyet_doi(self):
        # Đây CHÍNH LÀ ca làm lộ bug: topic `merchants` cả đời 50 message, Bronze
        # đã cam kết hết. Ngưỡng cũ "margin < 50.000" coi đây là NGUY HIỂM.
        assert retention_margin_ratio(earliest=0, committed=50, latest=50) == 1.0

    def test_sat_mep_retention_thi_ve_0(self):
        # Kafka đã xoá tới offset 900, Bronze mới cam kết tới 900 -> không còn đệm.
        assert retention_margin_ratio(earliest=900, committed=900, latest=1000) == 0.0

    def test_topic_rong_khong_canh_bao(self):
        # Không có gì để mất thì không có gì để cảnh báo. Nếu trả 0.0 ở đây thì mọi
        # topic mới tạo đều bắn alert ngay lúc sinh ra.
        assert retention_margin_ratio(earliest=0, committed=0, latest=0) == 1.0

    def test_khong_chia_cho_khong(self):
        """Cửa sổ âm (offset trồi sụt do đọc hai lần không nguyên tử) không được nổ."""
        assert retention_margin_ratio(earliest=100, committed=100, latest=50) == 1.0

    def test_ty_le_doc_lap_voi_co_topic(self):
        """Cùng một mức an toàn phải cho cùng một con số, bất kể topic to hay nhỏ.

        Đây là toàn bộ lý do đổi sang tỷ lệ: một ngưỡng phải dùng được cho MỌI topic.
        """
        nho = retention_margin_ratio(earliest=0, committed=25, latest=50)
        to = retention_margin_ratio(earliest=0, committed=1_500_000, latest=3_000_000)
        assert nho == to == 0.5


class TestRender:
    """Định dạng phơi ra cho Prometheus — sai một ký tự là collector vứt CẢ file."""

    def test_co_help_va_type_dung_mot_lan_moi_metric(self):
        out = render([
            ("gtl_kafka_lag_messages", {"topic": "a"}, 1),
            ("gtl_kafka_lag_messages", {"topic": "b"}, 2),
        ])
        # HELP/TYPE lặp lại cho cùng một metric là LỖI PARSE với Prometheus.
        assert out.count("# TYPE gtl_kafka_lag_messages") == 1
        assert out.count("# HELP gtl_kafka_lag_messages") == 1

    def test_metric_khong_nhan_khong_co_ngoac_rong(self):
        # `gtl_exporter_up{} 1` là hợp lệ nhưng xấu; quan trọng hơn là phải có
        # KHOẢNG TRẮNG giữa tên và giá trị, thiếu là parse hỏng.
        assert "gtl_exporter_up 1" in render([("gtl_exporter_up", {}, 1)])

    def test_nhan_duoc_boc_ngoac_kep(self):
        out = render([("gtl_kafka_lag_messages", {"topic": "gtl.public.x"}, 7)])
        assert 'gtl_kafka_lag_messages{topic="gtl.public.x"} 7' in out

    def test_ket_thuc_bang_xuong_dong(self):
        """Thiếu newline cuối file làm dòng cuối bị dính vào file .prom kế tiếp."""
        assert render([("gtl_exporter_up", {}, 1)]).endswith("\n")


class TestLoadEnv:
    """Đọc .env — nơi credential đi ra, nên phải chắc chắn về cách phân tách."""

    def test_giu_nguyen_gia_tri_co_dau_bang(self, tmp_path):
        """Secret key AWS RẤT hay chứa `=` (base64 padding).

        Cắt bằng `split("=")` thay vì `partition("=")` là mất phần đuôi -> xác thực
        hỏng với một thông báo lỗi chẳng liên quan gì tới nguyên nhân.
        """
        from gtl_session import load_env
        f = tmp_path / ".env"
        f.write_text("AWS_SECRET_ACCESS_KEY=abc/def+ghi=\nS3_BUCKET=b\n")
        # noqa S105: chuỗi giả trong file tạm của test, không phải credential thật.
        assert load_env(f)["AWS_SECRET_ACCESS_KEY"] == "abc/def+ghi="  # noqa: S105

    def test_bo_qua_comment_va_dong_trong(self, tmp_path):
        from gtl_session import load_env
        f = tmp_path / ".env"
        f.write_text("# ghi chú\n\nS3_BUCKET=x\n   \n# =không phải biến\n")
        assert load_env(f) == {"S3_BUCKET": "x"}


@pytest.mark.parametrize(
    "location,expected",
    [
        ("s3://buck/warehouse/bronze/tbl", "warehouse/bronze/tbl/"),
        # Iceberg đôi khi trả location có dấu / cuối, đôi khi không.
        ("s3://buck/warehouse/bronze/tbl/", "warehouse/bronze/tbl/"),
        ("s3://buck/warehouse", "warehouse/"),
    ],
)
def test_s3_prefix(location, expected):
    """Prefix này cấp danh sách file cho một thủ tục XOÁ FILE.

    Sai một ký tự = liệt kê nhầm thư mục = đưa Iceberg một danh sách sai. Thà sai
    ở đây bị unit test bắt còn hơn phát hiện bằng data đã mất.
    """
    from maintenance import s3_prefix
    assert s3_prefix(location, "buck") == expected
