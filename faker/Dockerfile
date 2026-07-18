# Slim image — faker chỉ cần Python + psycopg + Faker, giữ RAM thấp (limit 256M).
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY generate_transactions.py .

# -u: unbuffered stdout để log ra ngay (docker logs thấy tiến trình realtime).
CMD ["python", "-u", "generate_transactions.py"]
