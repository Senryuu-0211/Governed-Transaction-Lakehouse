"""Các tool agent được phép gọi. KHÔNG có tool nào nhận SQL.

VÌ SAO CHỌN TOOL CHỨ KHÔNG SINH SQL
  "Sinh SQL rồi chạy" nghe linh hoạt hơn, nhưng nó đặt tính đúng của mọi câu trả
  lời vào tay một mô hình ngôn ngữ. Các cách sai đều IM LẶNG: quên lọc chuyển
  khoản nội bộ, lấy trung bình của các tỷ lệ, gộp nhầm grain, join nhân đôi tiền.
  Kết quả vẫn là một con số trông hợp lý, và không ai kiểm.
  Ở đây mô hình chỉ CHỌN một tool và điền tham số; tầng semantic dựng câu lệnh.
  Những cách sai kia không diễn đạt được nữa.

VÌ SAO MÔ TẢ TOOL DÀI
  Mô tả chính là giao diện. Mô hình chọn sai tool gần như luôn là vì mô tả không
  nói rõ tool đó trả lời CÂU HỎI NÀO — không phải vì mô hình kém.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from langchain_core.tools import tool

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "semantic"))

import tools as analysis  # noqa: E402
from layer import SemanticLayer  # noqa: E402
from query import run as run_query  # noqa: E402

LAYER = SemanticLayer.load()

# Mỗi kết quả tool được cất vào đây kèm một mã. Câu trả lời cuối được đối chiếu
# NGƯỢC LẠI với chỗ này — con số không truy về được một fact nào là con số bịa.
FACTS: list[dict[str, Any]] = []


def _record(kind: str, payload: dict[str, Any]) -> dict[str, Any]:
    payload = dict(payload)
    payload["fact_id"] = f"F{len(FACTS) + 1}"
    payload["tool"] = kind
    FACTS.append(payload)
    return payload


def reset_facts() -> None:
    FACTS.clear()


@tool
def list_metrics() -> list[dict[str, Any]]:
    """Liệt kê mọi chỉ số đo được và các chiều cắt của từng chỉ số.

    GỌI CÁI NÀY TRƯỚC khi dùng bất kỳ tool nào khác, ở lượt đầu tiên. Tên chỉ số
    và tên chiều phải lấy nguyên văn từ đây — đoán tên sẽ bị từ chối.
    """
    return LAYER.describe()


@tool
def get_metric(
    metric: str,
    group_by: list[str] | None = None,
    filters: dict[str, Any] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Lấy giá trị một chỉ số, có thể tách theo chiều và lọc.

    Dùng cho câu hỏi "bao nhiêu": doanh số tháng 8, số giao dịch theo kênh, top
    merchant. KHÔNG dùng để so hai kỳ — dùng compare_periods, nó gác thêm những
    chỗ phép so kỳ hay nói dối.

    Ngày theo định dạng YYYY-MM-DD.
    """
    r = run_query(LAYER, metric, group_by=group_by, filters=filters,
                  date_from=date_from, date_to=date_to, limit=limit)
    return _record("get_metric", r)


@tool
def compare_periods(
    metric: str,
    base_from: str, base_to: str,
    comp_from: str, comp_to: str,
    group_by: list[str] | None = None,
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """So MỘT chỉ số giữa hai kỳ. `base` là kỳ trước, `comp` là kỳ đang xét.

    Dùng cho "tháng này so với tháng trước", "tuần rồi có gì khác". Tự cảnh báo
    khi hai kỳ dài khác nhau, lệch cơ cấu ngày trong tuần, rơi vào những phần
    khác nhau của tháng, hoặc chứa ngày hôm nay chưa đóng sổ.

    LUÔN đọc phần `warnings` và nói lại cho người dùng nếu có.
    """
    return _record("compare_periods", analysis.compare_periods(
        LAYER, metric, base_from, base_to, comp_from, comp_to,
        group_by=group_by, filters=filters))


@tool
def decompose_drivers(
    base_from: str, base_to: str, comp_from: str, comp_to: str,
) -> dict[str, Any]:
    """Tách thay đổi sản lượng thành BA NHÂN TỬ: số khách hoạt động mỗi ngày ×
    số giao dịch mỗi khách × giá trị mỗi giao dịch.

    Đây là tool trả lời "VÌ SAO tăng/giảm" ở mức toàn hệ thống. Ba nhân tử di
    chuyển độc lập nhau nên cái nào đóng góp nhiều nhất chính là nguyên nhân.
    Dùng LMDI nên tổng ba đóng góp đúng bằng mức thay đổi (`residual` ≈ 0).

    KHÔNG lọc được theo vùng/kênh/nhóm khách: "số khách hoạt động" chỉ đúng ở
    grain ngày toàn hệ thống. Muốn đi sâu theo chiều thì dùng explain_change.
    """
    return _record("decompose_drivers", analysis.decompose_drivers(
        LAYER, base_from, base_to, comp_from, comp_to))


@tool
def explain_change(
    metric: str, dimension: str,
    base_from: str, base_to: str, comp_from: str, comp_to: str,
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """AI gây ra mức thay đổi: xếp hạng từng giá trị của một chiều theo đóng góp.

    Dùng khi đã biết "giảm bao nhiêu" và cần biết "do ai". Trả về cả
    `net_change` (thay đổi thuần) lẫn `gross_movement` (tổng biến động) — nếu
    gross lớn hơn hẳn net thì các nhóm đang BÙ TRỪ nhau, và đó thường mới là
    điều đáng báo cáo nhất. Cờ `disappeared` nghĩa là nhóm đó biến mất hẳn.
    """
    return _record("explain_change", analysis.explain_change(
        LAYER, metric, dimension, base_from, base_to, comp_from, comp_to,
        filters=filters))


@tool
def detect_divergence(
    metric: str, dimension: str,
    base_from: str, base_to: str, comp_from: str, comp_to: str,
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Nhóm nào đi LỆCH khỏi phần còn lại (difference-in-differences).

    Dùng khi nghi một kênh/vùng/nhóm có vấn đề RIÊNG. Khác explain_change: ở đây
    mỗi nhóm được so với chính phần còn lại, nên một đợt suy giảm toàn thị
    trường sẽ KHÔNG bị gắn cờ, còn sự cố của riêng một nhóm thì có.

    Chỉ những dòng có `significant: true` mới đáng gọi là bất thường. Đã quét
    nhiều nhóm mà không dòng nào significant thì kết luận là "không có gì lạ".
    """
    return _record("detect_divergence", analysis.detect_divergence(
        LAYER, metric, dimension, base_from, base_to, comp_from, comp_to,
        filters=filters))


@tool
def find_concentration(
    metric: str, dimension: str,
    date_from: str | None = None, date_to: str | None = None,
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Mức TẬP TRUNG: một vài kẻ chi phối hay phân tán đều?

    Trả HHI (thang 0–10.000 của cơ quan chống độc quyền Mỹ) và
    `effective_entities` = "tương đương bao nhiêu đối tác đều nhau" — dùng con số
    thứ hai khi nói với người không rành kỹ thuật, nó dễ hình dung hơn HHI thô.
    """
    return _record("find_concentration", analysis.find_concentration(
        LAYER, metric, dimension, date_from=date_from, date_to=date_to,
        filters=filters))


@tool
def find_dropouts(
    metric: str, dimension: str, date_from: str, date_to: str,
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Ai NGỪNG hoạt động, từ ngày nào tới ngày nào — tìm khoảng IM LẶNG liên tiếp.

    DÙNG CÁI NÀY cho mọi câu hỏi kiểu "có ai ngừng giao dịch / tắt / biến mất /
    gián đoạn không". Với merchant: metric='merchant_txn_count',
    dimension='merchant_name'.

    ⚠️ KHÔNG dùng compare_periods hay explain_change cho loại câu hỏi này: chúng gộp
    theo kỳ, nên một khoảng im lặng cắt ngang ranh giới hai kỳ bị pha loãng hoặc
    ĐẢO DẤU — đã xảy ra thật: một merchant tắt 8 ngày bị báo là TĂNG +33 triệu.
    Tool này nhìn chuỗi ngày liền mạch nên không có ranh giới nào để cắt ngang.
    Trả ngày bắt đầu/kết thúc chính xác và ước lượng khối lượng mất đi.
    """
    return _record("find_dropouts", analysis.find_dropouts(
        LAYER, metric, dimension, date_from, date_to, filters=filters))


ALL_TOOLS = [
    list_metrics, get_metric, compare_periods,
    decompose_drivers, explain_change, detect_divergence, find_concentration,
    find_dropouts,
]
