import argparse
import uvicorn
from fastapi import FastAPI

parser = argparse.ArgumentParser()
parser.add_argument("--node-id", type=int, required=True)
parser.add_argument("--port", type=int, required=True)

args = parser.parse_args()

app = FastAPI()

NODE_ID = args.node_id


@app.get("/health")
def health():
    return {
        "node_id": NODE_ID,
        "status": "ok"
    }


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=args.port
    )