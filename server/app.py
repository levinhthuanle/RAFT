import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from raft import RaftNode
from const import NODES


def create_app(node_id: int) -> FastAPI:
    peers = {nid: url for nid, url in NODES.items() if nid != node_id}
    node = RaftNode(node_id, peers)
    print(node)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        asyncio.create_task(node.run_election_timer())
        yield

    app = FastAPI(lifespan=lifespan)

    @app.get("/health")
    def health():
        return node.get_status()

    return app
