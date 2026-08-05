# Issue #001 — Xung đột port observability (9090 / 3000)

**Trạng thái:** ✅ ĐÃ ĐÓNG (01-08) — chọn phương án 1, xem cuối file
**Phát hiện:** Phase 1 Step 1a (2026-07-08)
**Ảnh hưởng:** Phase 5 (Observability) — CHƯA chặn hiện tại
**Mức:** Medium (kiến trúc, cần quyết định trước Phase 5)

## Vấn đề
`CLAUDE.md` của project dành **Prometheus :9090** và **Grafana :3000** cho observability
(Phase 5), giả định "Grafana dùng chung 3000 của Project 1".

Nhưng trên home server hiện tại, **monitoring stack riêng đã chiếm 9090 + 3000**
(`~/working/projects/Resource-Monitoring-Dashboard/`), bind vào Tailscale IP
`100.71.245.124`. Dựng thêm Prometheus/Grafana ở cùng port sẽ **xung đột bind**.

```
$ ss -ltn | grep -E ':9090|:3000'
100.71.245.124:3000   (Grafana - monitoring stack)
100.71.245.124:9090   (Prometheus - monitoring stack)
```

## Hướng xử lý (quyết định ở Phase 5)
1. **Tái dùng monitoring stack có sẵn (khuyến nghị):** thêm scrape target + dashboard của
   banking pipeline vào Prometheus/Grafana đang chạy, thay vì dựng bộ mới.
   → Tiết kiệm RAM (server đã đông), một nơi xem tất cả. Đúng tinh thần không over-provision.
2. **Prometheus riêng cho project ở port khác** (vd 9091) + Grafana riêng port khác (vd 3001):
   cách ly hoàn toàn nhưng tốn thêm RAM và phân mảnh nơi xem.

## Ghi chú
- Loki (:3100) mà plan dự định thì hiện chưa ai chiếm → OK.
- Xem thêm `PORTS.md` (bảng port tổng).


---

## ✅ Đã đóng 01-08-2026 — chọn phương án 1

Phase 5 làm đúng **phương án 1** đề xuất ở trên: **tái dùng monitoring stack có sẵn**, không
dựng Prometheus/Grafana thứ hai. Cụ thể:

- `scripts/metrics_exporter.py` ghi file `.prom` → **textfile collector của `node_exporter`
  đang chạy sẵn 24/7** đọc → Prometheus → Grafana. Không thêm cổng nào, không thêm daemon nào.
- Dashboard `GTL · Pipeline Health` + 9 luật alert nằm trong Grafana `:3000` đang có.
- Đã kiểm chứng **không phá thứ đang chạy**: sau khi restart `node-exporter`, dashboard host vẫn
  đủ **1.354 series** cpu/mem/disk/net, và `node_textfile_scrape_error = 0`.

Xung đột port biến mất vì **không có gì mới cần bind port**. Đúng như ghi chú "tiết kiệm RAM,
một nơi xem tất cả, không over-provision".

Chi tiết: `docs/observability.md`.
