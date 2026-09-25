"""Agent hỏi-đáp: LLM chọn tool, code chạy truy vấn, code kiểm lại từng con số.

ĐỒ THỊ
    agent ──(có tool call)──> tools ──> agent ──(hết tool call)──> verify ──> END

VÌ SAO DỰNG ĐỒ THỊ TAY THAY VÌ create_react_agent
  Cần một nút `verify` chạy SAU khi mô hình đã viết xong câu trả lời, và cần trần
  số vòng lặp. Hai thứ đó không nằm trong vòng ReAct dựng sẵn — mà chính chúng
  mới là phần khiến agent này khác một con chatbot gọi hàm.

VÌ SAO temperature = 0
  Đây là trợ lý cho số liệu tài chính. Cùng một câu hỏi phải ra cùng một câu trả
  lời; "sáng tạo" ở đây chỉ có nghĩa là không tái lập được.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Annotated, Any, TypedDict

from langchain_core.messages import AnyMessage, SystemMessage, ToolMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

sys.path.insert(0, str(Path(__file__).resolve().parent))

from tool_defs import ALL_TOOLS, FACTS, reset_facts  # noqa: E402
from verify import verify_answer  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Trần vòng lặp. Không có nó, một mô hình bí sẽ gọi tool mãi — vừa tốn tiền vừa
# không bao giờ trả lời. Thà dừng và nói "chưa đủ dữ kiện".
MAX_STEPS = 8

SYSTEM_PROMPT = """\
Bạn là trợ lý phân tích dữ liệu giao dịch của một ngân hàng. Người hỏi thường
KHÔNG rành kỹ thuật — họ cần một câu trả lời đọc được, có số, và kiểm lại được.

LUẬT BẮT BUỘC
1. MỌI con số trong câu trả lời phải đến từ kết quả tool. Không ước lượng, không
   nhớ lại, không suy đoán. Không có dữ liệu thì nói thẳng là không có.
2. Gọi `list_metrics` TRƯỚC TIÊN ở lượt đầu. Tên chỉ số và tên chiều phải lấy
   nguyên văn từ đó.
3. Nếu kết quả tool có `warnings`, PHẢI nói lại cho người dùng. Một con số kèm
   cảnh báo còn dùng được; một con số giấu cảnh báo thì không.
4. Câu hỏi "vì sao tăng/giảm" thì đừng dừng ở con số: gọi tiếp `decompose_drivers`
   (nhân tử) hoặc `explain_change` (ai gây ra) hoặc `detect_divergence` (có phải
   riêng nhóm này không).
5. Với `detect_divergence`, chỉ gọi là bất thường khi `significant` = true.
   Câu hỏi "ai ngừng / tắt / biến mất / gián đoạn" thì dùng `find_dropouts` — so kỳ
   sẽ pha loãng hoặc đảo dấu một khoảng im lặng cắt ngang ranh giới kỳ.
6. Trả lời bằng tiếng Việt, ngắn gọn. Nêu con số trước, giải thích sau.
7. KHÔNG được lập luận vượt qua một cảnh báo. Nếu tool cảnh báo rằng chênh lệch
   có thể do chu kỳ hoặc mùa vụ, bạn KHÔNG được viện dẫn con số khác để bác bỏ
   nó — hãy nói thẳng là chưa kết luận được, hoặc gọi lại tool với hai kỳ cùng
   vị trí trong tháng. Dòng nào có `seasonal` = true là NHỊP CHU KỲ, tuyệt đối
   không gọi là sự cố.
8. Kết thúc bằng một dòng: "Số liệu tính đến offset Kafka <watermark>." lấy từ
   trường `watermark` của kết quả tool cuối cùng.

Hôm nay là {today}. Dữ liệu có khoảng 90 ngày gần nhất.
"""


class State(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]
    steps: int
    verification: dict[str, Any] | None


def _read_key() -> str:
    if os.environ.get("DEEPSEEK_API_KEY"):
        return os.environ["DEEPSEEK_API_KEY"]
    env = PROJECT_ROOT / ".env"
    for line in env.read_text(encoding="utf-8").splitlines():
        if line.strip().startswith("DEEPSEEK_API_KEY="):
            return line.split("=", 1)[1].strip()
    raise RuntimeError("thiếu DEEPSEEK_API_KEY trong .env")


def build_llm() -> ChatOpenAI:
    # DeepSeek nói đúng giao thức OpenAI nên dùng chung client — đổi sang nhà cung
    # cấp khác chỉ là đổi base_url và tên model, không sửa code agent.
    return ChatOpenAI(
        model=os.environ.get("DEEPSEEK_MODEL", "deepseek-chat"),
        api_key=_read_key(),
        base_url="https://api.deepseek.com",
        temperature=0,
        timeout=120,
        max_retries=2,
    )


def build_graph():
    llm = build_llm().bind_tools(ALL_TOOLS)

    def agent(state: State) -> dict[str, Any]:
        msgs = state["messages"]
        if not any(isinstance(m, SystemMessage) for m in msgs):
            from datetime import date
            msgs = [SystemMessage(SYSTEM_PROMPT.format(today=date.today()))] + list(msgs)
        return {"messages": [llm.invoke(msgs)], "steps": state.get("steps", 0) + 1}

    def route(state: State) -> str:
        last = state["messages"][-1]
        if getattr(last, "tool_calls", None):
            if state.get("steps", 0) >= MAX_STEPS:
                # Dừng có kiểm soát thay vì để vòng lặp chạy tiếp: câu trả lời
                # thiếu mà nói rõ là thiếu vẫn tốt hơn một hoá đơn API không đáy.
                return "verify"
            return "tools"
        return "verify"

    def verify(state: State) -> dict[str, Any]:
        """Đối chiếu từng con số trong câu trả lời ngược về kết quả tool."""
        text = ""
        for m in reversed(state["messages"]):
            if not isinstance(m, ToolMessage) and getattr(m, "content", None):
                text = m.content if isinstance(m.content, str) else str(m.content)
                break
        return {"verification": verify_answer(text, FACTS)}

    g = StateGraph(State)
    g.add_node("agent", agent)
    g.add_node("tools", ToolNode(ALL_TOOLS))
    g.add_node("verify", verify)
    g.set_entry_point("agent")
    g.add_conditional_edges("agent", route, {"tools": "tools", "verify": "verify"})
    g.add_edge("tools", "agent")
    g.add_edge("verify", END)
    return g.compile()


def ask(question: str) -> dict[str, Any]:
    """Hỏi một câu, trả về câu trả lời kèm dấu vết kiểm chứng."""
    reset_facts()
    graph = build_graph()
    out = graph.invoke(
        {"messages": [("user", question)], "steps": 0, "verification": None},
        {"recursion_limit": MAX_STEPS * 3},
    )
    answer = ""
    tool_calls: list[str] = []
    for m in out["messages"]:
        if getattr(m, "tool_calls", None):
            tool_calls += [tc["name"] for tc in m.tool_calls]
        if not isinstance(m, ToolMessage) and getattr(m, "content", None):
            if isinstance(m.content, str) and m.content.strip():
                answer = m.content
    return {
        "question": question,
        "answer": answer,
        "tools_used": tool_calls,
        "verification": out.get("verification"),
        "facts": list(FACTS),
    }
