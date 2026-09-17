import asyncio
import random
import httpx


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

        self._loop = asyncio.get_event_loop()
        self.last_heartbeat = self._loop.time()

    def get_status(self):
        return {
            "node_id": self.node_id,
            "state": self.state,
            "term": self.current_term,
            "voted_for": self.voted_for,
        }

    def reset_election_timer(self):
        self.last_heartbeat = self._loop.time()

    # --- Election Timer ---

    async def run_election_timer(self):
        while True:
            timeout = random.uniform(0.15, 0.3)
            await asyncio.sleep(timeout)

            if self.state == "leader":
                continue

            elapsed = self._loop.time() - self.last_heartbeat
            if elapsed >= timeout:
                await self.start_election()

    # --- Election ---

    async def start_election(self):
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        votes = 1  # tự vote cho mình
        print(f"[Node {self.node_id}] Starting election for term {self.current_term}")

        # Gửi RequestVote đến tất cả peers song song
        tasks = [self._send_vote_request(peer_url) for peer_url in self.peers.values()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                continue
            if result.get("term", 0) > self.current_term:
                # Peer có term cao hơn → mình outdated, về follower
                self._become_follower(result["term"])
                return
            if result.get("vote_granted"):
                votes += 1

        majority = (len(self.peers) + 1) // 2 + 1
        if self.state == "candidate" and votes >= majority:
            self._become_leader()

    async def _send_vote_request(self, peer_url: str) -> dict:
        payload = {"term": self.current_term, "candidate_id": self.node_id}
        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{peer_url}/request_vote", json=payload, timeout=0.1)
            return resp.json()

    # --- Vote Handler (được gọi từ endpoint) ---

    def handle_vote_request(self, term: int, candidate_id: int) -> dict:
        # Nếu term của candidate thấp hơn → từ chối
        if term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}

        # Nếu term cao hơn → cập nhật term, về follower
        if term > self.current_term:
            self._become_follower(term)

        # Vote nếu chưa vote cho ai, hoặc đã vote cho chính candidate này
        can_vote = self.voted_for is None or self.voted_for == candidate_id
        if can_vote:
            self.voted_for = candidate_id
            self.reset_election_timer()
            print(f"[Node {self.node_id}] Voted for node {candidate_id} in term {term}")
            return {"term": self.current_term, "vote_granted": True}

        return {"term": self.current_term, "vote_granted": False}

    # --- Leader ---

    def _become_leader(self):
        self.state = "leader"
        print(f"[Node {self.node_id}] Became LEADER for term {self.current_term}")
        asyncio.create_task(self._send_heartbeats())

    async def _send_heartbeats(self):
        while self.state == "leader":
            tasks = [self._send_heartbeat(peer_url) for peer_url in self.peers.values()]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(0.05)  # 50ms

    async def _send_heartbeat(self, peer_url: str):
        payload = {"term": self.current_term, "leader_id": self.node_id}
        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{peer_url}/append_entries", json=payload, timeout=0.1)
            data = resp.json()
            # Nếu peer có term cao hơn → mình outdated
            if data.get("term", 0) > self.current_term:
                self._become_follower(data["term"])

    # --- AppendEntries Handler (được gọi từ endpoint) ---

    def handle_append_entries(self, term: int, leader_id: int) -> dict:
        if term < self.current_term:
            return {"term": self.current_term, "success": False}

        if term > self.current_term:
            self._become_follower(term)

        self.reset_election_timer()
        return {"term": self.current_term, "success": True}

    # --- Helpers ---

    def _become_follower(self, term: int):
        self.state = "follower"
        self.current_term = term
        self.voted_for = None
        self.reset_election_timer()
        print(f"[Node {self.node_id}] Became follower for term {term}")
