{{ config(materialized='table') }}

-- Dimension kênh giao dịch — enum nhỏ, làm giàu bằng thuộc tính phân tích
-- (digital vs physical) để business cắt "tỷ trọng kênh số" không cần nhớ mã.

select channel, is_digital, requires_presence
from values
    ('ATM',    false, true),
    ('POS',    false, true),
    ('ONLINE', true,  false),
    ('MOBILE', true,  false),
    ('BRANCH', false, true)
as t(channel, is_digital, requires_presence)
