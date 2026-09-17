from fastapi import FastAPI
from raft import RaftNode
from const import NODES


def create_app(node_id: int) -> FastAPI:
    peers = {nid: url for nid, url in NODES.items() if nid != node_id}
    node = RaftNode(node_id, peers)

    app = FastAPI()

    @app.get("/health")
    def health():
        return node.get_status()

    return app
