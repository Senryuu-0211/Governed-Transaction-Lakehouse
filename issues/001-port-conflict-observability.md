# Issue #001 — Xung đột port observability (9090 / 3000)

**Trạng thái:** OPEN
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
