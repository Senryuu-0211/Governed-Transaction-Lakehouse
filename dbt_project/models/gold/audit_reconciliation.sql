{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    full_refresh=false
) }}

-- SỔ ĐỐI SOÁT (append-only) — bằng chứng "tiền vào = tiền ra" của MỖI lần chạy.
--
-- VÌ SAO CẦN, khi đã có 70+ dbt test:
--   Test thường kiểm TỪNG DÒNG (unique/not_null/relationships). Nhưng với tiền,
--   từng dòng đúng vẫn có thể MẤT nguyên một khối: transform lọc nhầm, join hụt,
--   MERGE ghi đè sai -> mọi test vẫn xanh mà tổng tiền lệch. Chỉ đối soát TỔNG
--   giữa hai đầu pipeline mới bắt được. Ngân hàng gọi đây là reconciliation:
--   một xu lệch là một xu phải truy ra.
--
-- VÌ SAO TÍNH LẠI TỪ BRONZE THÔ (không tái dùng Silver):
--   Nếu lấy số từ Silver rồi so với Gold thì cả hai cùng đi qua một đường
--   transform -> lỗi chung sẽ triệt tiêu nhau và ta đối soát với chính mình.
--   Ở đây Bronze được tính ĐỘC LẬP từ CDC thô, nên đường Silver->Gold bị soi
--   bởi một phép tính không dùng chung code với nó.
--
-- ⚠️ CỬA SỔ ĐỐI SOÁT (đổi 01-08 — quan trọng về CHI PHÍ):
--   Bản đầu quét TOÀN BỘ lịch sử Bronze mỗi lần chạy. Trên MinIO thì miễn phí;
--   trên S3 THẬT, compute chạy on-prem nên mỗi byte đọc là data-transfer-out
--   TÍNH TIỀN -> quét toàn bộ Bronze hàng giờ ≈ 36TB egress ≈ $3.000/tháng.
--   Giờ chỉ đối soát các giao dịch ĐƯỢC NẠP trong `recon_window_days` ngày gần
--   nhất; Bronze phân vùng theo days(ingest_ts) nên bộ lọc này PRUNE PARTITION,
--   đọc ~5GB thay vì toàn bộ kho.
--   Ngữ nghĩa đổi từ "toàn lịch sử khớp" -> "mọi thứ nạp gần đây khớp", và đúng
--   chuẩn kiểm toán hơn: mỗi ngày được chứng minh TẠI THỜI ĐIỂM đó rồi ghi vĩnh
--   viễn vào sổ; không ai đi chứng minh lại năm 2019 mỗi sáng.
--
-- VÌ SAO append-only + full_refresh=false:
--   Đây là chứng cứ kiểm toán. `dbt build --full-refresh` (mình chạy thường xuyên
--   sau reset) sẽ DỰNG LẠI bảng và xoá sạch lịch sử -> mất chứng cứ. Cờ
--   full_refresh=false khiến model PHỚT LỜ --full-refresh: bảo vệ bằng CODE,
--   không phải bằng lời dặn "nhớ đừng".

{% set window_days = var('recon_window_days', 2) %}

with watermark as (
    -- MỐC ĐỐI SOÁT — điểm mấu chốt của reconciliation trên pipeline STREAMING.
    --
    -- Bronze được stream nạp liên tục, còn Gold chỉ đóng băng tại lần dbt chạy gần
    -- nhất. So "Bronze lúc này" với "Gold lúc nãy" là so mục tiêu di động với ảnh
    -- chụp -> luôn lệch, và lệch to dần theo thời gian (đo thật: 31.253 giao dịch).
    -- Đó là ảo ảnh do thời điểm, KHÔNG phải mất tiền.
    --
    -- Cách đúng: đối soát AS-OF một điểm nhất quán. Silver ghi lại offset Kafka
    -- cuối cùng nó đã tiêu thụ (_kafka_offset), và Gold dựng từ Silver -> lấy
    -- chính offset đó làm mốc, rồi chỉ tính phần Bronze NẰM TRONG mốc.
    -- Giờ hai vế cùng nhìn một lát cắt dữ liệu, lệch ≠ 0 mới thật sự là mất/lệch tiền.
    select coalesce(max(_kafka_offset), -1) as max_offset
    from {{ ref('transactions') }}
),

bronze_latest as (
    -- Collapse CDC log -> trạng thái CUỐI CÙNG của mỗi giao dịch TRONG CỬA SỔ,
    -- tính đến watermark.
    -- KHÔNG dùng macro cdc_latest_events: macro đó có nhánh is_incremental() gắn
    -- với {{ this }} (bảng Silver); ở đây cần quét theo cửa sổ riêng.
    -- Grain phải KHỚP Gold: Bronze thô có NHIỀU dòng/giao dịch (mỗi lần đổi trạng
    -- thái là 1 event), Gold có 1 dòng/giao dịch. So thẳng Bronze thô với Gold là
    -- so khác grain -> luôn lệch, vô nghĩa.
    -- Lọc offset TRƯỚC rồi mới dedup: phải lấy trạng thái cuối *mà Silver đã thấy*,
    -- không phải trạng thái mới nhất hiện giờ.
    select
        cast(get_json_object(key, '$.txn_id') as bigint)    as txn_id,
        get_json_object(value, '$.after.status')            as status,
        coalesce(get_json_object(value, '$.after.amount'),
                 get_json_object(value, '$.before.amount')) as amount_str,
        op
    from (
        select key, value, op,
               row_number() over (partition by key order by kafka_offset desc) as _rn
        from {{ source('bronze', 'transactions') }}, watermark
        where value is not null
          and op in ('r', 'c', 'u', 'd')
          and kafka_offset <= watermark.max_offset
          -- ⬇ BỘ LỌC CẮT CHI PHÍ: prune partition days(ingest_ts).
          and ingest_ts >= date_sub(current_date(), {{ window_days }})
    )
    where _rn = 1
),

bronze_live as (
    -- op='d' = giao dịch đã bị purge ở nguồn; bên Gold cũng loại (is_deleted).
    select * from bronze_latest where op != 'd'
),

bronze_agg as (
    select
        count(*) as count_bronze,
        -- Chỉ COMPLETED mới là tiền thật -> khớp đúng định nghĩa net_amount bên Gold.
        coalesce(sum(case when status = 'COMPLETED'
                          then cast(amount_str as decimal(15,2))
                          else 0 end), 0) as amount_bronze
    from bronze_live
),

gold_agg as (
    -- Chỉ lấy ĐÚNG những giao dịch có trong cửa sổ Bronze -> hai vế cùng tập hợp.
    -- Fact GIỮ dòng soft-delete (không xoá dấu vết) nên phải lọc is_deleted.
    select
        count(*) as count_gold,
        coalesce(sum(net_amount), 0) as amount_gold
    from {{ ref('fact_transactions') }} f
    where not f.is_deleted
      and f.txn_id in (select txn_id from bronze_live)
)

select
    current_timestamp()                 as run_ts,
    current_date()                      as run_date,
    {{ window_days }}                   as window_days,
    w.max_offset                        as watermark_offset,
    b.count_bronze,
    g.count_gold,
    b.count_bronze - g.count_gold       as count_diff,
    b.amount_bronze,
    g.amount_gold,
    b.amount_bronze - g.amount_gold     as amount_diff
from bronze_agg b
cross join gold_agg g
cross join watermark w
