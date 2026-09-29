FROM python:3.11-slim

WORKDIR /app

COPY Requirements.txt .
RUN pip install --no-cache-dir -r Requirements.txt psycopg2-binary

# Generate gRPC stubs from proto definition
COPY proto/raft.proto ./proto/
RUN python -m grpc_tools.protoc \
    -I./proto \
    --python_out=. \
    --grpc_python_out=. \
    proto/raft.proto

COPY server/ .

CMD ["python", "main.py", "--node-id", "1", "--port", "8000"]
