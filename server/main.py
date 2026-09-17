import argparse
import uvicorn
from app import create_app

parser = argparse.ArgumentParser()
parser.add_argument("--node-id", type=int, required=True)
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()

app = create_app(args.node_id)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=args.port)