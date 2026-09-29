import os


def get_peers(node_id: int) -> dict[int, str]:
    # PEERS="http://node1:8000,http://node2:8000,http://node3:8000"
    # Thứ tự trong list = node_id (index+1)
    peers_env = os.getenv("PEERS")
    if peers_env:
        peers = {}
        for idx, url in enumerate(peers_env.split(","), start=1):
            url = url.strip()
            if idx != node_id and url:
                peers[idx] = url
        return peers

    # Fallback local
    all_nodes = {1: "http://127.0.0.1:8001", 2: "http://127.0.0.1:8002", 3: "http://127.0.0.1:8003"}
    return {nid: url for nid, url in all_nodes.items() if nid != node_id}


def get_grpc_peers(node_id: int) -> dict[int, str]:
    # GRPC_PEERS="node1:50051,node2:50051,node3:50051"
    grpc_peers_env = os.getenv("GRPC_PEERS")
    if grpc_peers_env:
        peers = {}
        for idx, addr in enumerate(grpc_peers_env.split(","), start=1):
            addr = addr.strip()
            if idx != node_id and addr:
                peers[idx] = addr
        return peers

    # Fallback local
    all_nodes = {1: "127.0.0.1:9001", 2: "127.0.0.1:9002", 3: "127.0.0.1:9003"}
    return {nid: addr for nid, addr in all_nodes.items() if nid != node_id}

