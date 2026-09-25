"""Hỏi agent một câu từ dòng lệnh.

    ~/working/gtl-agent-venv/bin/python agent/cli.py "doanh số tháng 8 bao nhiêu?"

In kèm: tool đã gọi, kết quả kiểm số, và SQL — tôn chỉ "SQL luôn hiện cho người
dùng xem". Người không rành kỹ thuật sẽ bỏ qua, nhưng người phải KÝ vào báo cáo
thì đọc, và đó mới là người cần thuyết phục.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from graph import ask  # noqa: E402


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    show_sql = "--sql" in sys.argv
    question = " ".join(a for a in sys.argv[1:] if not a.startswith("--"))
    r = ask(question)

    print("=" * 72)
    print(r["answer"])
    print("=" * 72)
    print("tool đã gọi:", " -> ".join(r["tools_used"]) or "(không gọi tool nào)")

    v = r["verification"] or {}
    print(f"kiểm số: {v.get('verified', 0)} khớp · {v.get('derived', 0)} suy ra "
          f"· {len(v.get('unverified', []))} CHƯA TRUY ĐƯỢC")
    if v.get("unverified"):
        print("  ⚠️ không truy về kết quả tool nào:", ", ".join(v["unverified"]))
    if show_sql:
        for f in r["facts"]:
            for s in ([f["sql"]] if isinstance(f.get("sql"), str) else f.get("sql", [])):
                print("-" * 72)
                print(s)
    return 0


if __name__ == "__main__":
    sys.exit(main())
