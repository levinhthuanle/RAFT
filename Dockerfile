FROM python:3.11-slim

WORKDIR /app

COPY Requirements.txt .
RUN pip install --no-cache-dir -r Requirements.txt psycopg2-binary

COPY server/ .

CMD ["python", "main.py", "--node-id", "1", "--port", "8000"]
