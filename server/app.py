import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

import grpc
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

import raft_pb2_grpc
from grpc_servicer import RaftServicer
from raft import RaftNode
from models import ClientCommand, ClientResponse
from const import get_peers, get_grpc_peers


def _start_grpc_server(node: RaftNode) -> grpc.Server:
    grpc_port = int(os.getenv("GRPC_PORT", "50051"))
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServiceServicer_to_server(RaftServicer(node), server)
    server.add_insecure_port(f"[::]:{grpc_port}")
    server.start()
    print(f"[Node {node.node_id}] gRPC server on port {grpc_port}")
    return server


def create_app(node_id: int, data_dir: str = "data") -> FastAPI:
    peers = get_peers(node_id)
    grpc_peers = get_grpc_peers(node_id)
    database_url = os.getenv("DATABASE_URL")
    node = RaftNode(node_id, peers, grpc_peers=grpc_peers, data_dir=data_dir, database_url=database_url)
    print(node)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        grpc_server = _start_grpc_server(node)
        asyncio.create_task(node.run_election_timer())
        yield
        grpc_server.stop(grace=1)

    app = FastAPI(lifespan=lifespan)

    @app.get("/health")
    def health():
        return node.get_status()

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics():
        node._update_metrics()
        return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.post("/command", response_model=ClientResponse)
    async def command(req: ClientCommand):
        result = await node.append_command(req.command)
        if result is None:
            return {"success": False, "leader_id": node.voted_for}
        return {"success": True, "result": result}

    return app
