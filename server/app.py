import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from raft import RaftNode
from models import VoteRequest, VoteResponse, AppendEntriesRequest, AppendEntriesResponse
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

    @app.post("/request_vote", response_model=VoteResponse)
    def request_vote(req: VoteRequest):
        result = node.handle_vote_request(req.term, req.candidate_id)
        return result

    @app.post("/append_entries", response_model=AppendEntriesResponse)
    def append_entries(req: AppendEntriesRequest):
        result = node.handle_append_entries(req.term, req.leader_id)
        return result

    return app
