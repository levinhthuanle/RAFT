import argparse
import uvicorn
from app import create_app

parser = argparse.ArgumentParser()
parser.add_argument("--node-id", type=int, required=True)
parser.add_argument("--port", type=int, required=True)
parser.add_argument("--data-dir", default="data")
args = parser.parse_args()

app = create_app(args.node_id, args.data_dir)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=args.port)
