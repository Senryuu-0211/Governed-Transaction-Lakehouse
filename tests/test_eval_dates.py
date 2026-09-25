"""Bộ đọc ngày của eval: một câu trả lời ĐÚNG TỪNG NGÀY từng bị chấm là khoanh sai
vì bộ đọc không hiểu dạng 10/07/2026 (eval 25-09). Bộ chấm oan người đúng thì không
ai tin nó nữa."""
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "agent"))

from eval import dates_in, overlaps  # noqa: E402

W = (date(2026, 7, 10), date(2026, 7, 17))


def test_all_the_ways_an_answer_writes_a_date():
    assert date(2026, 7, 10) in dates_in("từ 10/07/2026", 2026)
    assert date(2026, 7, 10) in dates_in("ngày 2026-07-10", 2026)
    assert date(2026, 7, 10) in dates_in("ngày 10/07", 2026)
    assert {date(2026, 7, d) for d in range(23, 28)} <= dates_in("đợt 23–27/07", 2026)


def test_overlap_is_true_only_near_the_real_window():
    assert overlaps("Khoảng im lặng: 10/07/2026 → 17/07/2026", W)
    assert not overlaps("ngày 02/07/2026", W)
