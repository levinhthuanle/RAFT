import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from raft import RaftNode
from models import (
    VoteRequest, VoteResponse,
    AppendEntriesRequest, AppendEntriesResponse,
    ClientCommand, ClientResponse,
)
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
        result = node.handle_vote_request(req.term, req.candidate_id, req.last_log_index, req.last_log_term)
        return result

    @app.post("/append_entries", response_model=AppendEntriesResponse)
    def append_entries(req: AppendEntriesRequest):
        result = node.handle_append_entries(
            req.term,
            req.leader_id,
            req.prev_log_index,
            req.prev_log_term,
            [e.model_dump() for e in req.entries],
            req.leader_commit,
        )
        return result

    @app.post("/command", response_model=ClientResponse)
    async def command(req: ClientCommand):
        result = await node.append_command(req.command)
        if result is None:
            return {"success": False, "leader_id": node.voted_for}
        return {"success": True, "result": result}

    return app
