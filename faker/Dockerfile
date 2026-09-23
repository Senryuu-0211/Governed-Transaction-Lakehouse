# Slim image — faker chỉ cần Python + psycopg + Faker, giữ RAM thấp (limit 256M).
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# world.py PHẢI có mặt: generate_transactions.py import nó ở module level.
# Liệt kê tường minh thay vì `COPY . .` để không kéo __pycache__ vào image.
COPY world.py generate_transactions.py ./

# -u: unbuffered stdout để log ra ngay (docker logs thấy tiến trình realtime).
CMD ["python", "-u", "generate_transactions.py"]
