import asyncio
import random


class RaftNode:
    def __str__(self):
        return f"RaftNode(id={self.node_id}, state={self.state}, term={self.current_term}, peers={list(self.peers.keys())})"

    
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers

        self.state = "follower"
        self.current_term = 0
        self.voted_for = None

        self.log = []
        self.commit_index = -1

        self.last_heartbeat = asyncio.get_event_loop().time()

    def get_status(self):
        return {
            "node_id": self.node_id,
            "state": self.state,
            "term": self.current_term,
            "voted_for": self.voted_for,
        }

    def reset_election_timer(self):
        self.last_heartbeat = asyncio.get_event_loop().time()

    async def run_election_timer(self):
        while True:
            timeout = random.uniform(0.15, 0.3)
            await asyncio.sleep(timeout)

            if self.state == "leader":
                continue

            elapsed = asyncio.get_event_loop().time() - self.last_heartbeat
            if elapsed >= timeout:
                await self.start_election()

    async def start_election(self):
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        print(f"[Node {self.node_id}] Starting election for term {self.current_term}")