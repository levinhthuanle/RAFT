import asyncio
import random
import httpx
from state_machine import StateMachine


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
        self.last_applied = -1

        self.next_index = {}
        self.match_index = {}

        self.state_machine = StateMachine()

        self._loop = asyncio.get_event_loop()
        self.last_heartbeat = self._loop.time()

    def get_status(self):
        return {
            "node_id": self.node_id,
            "state": self.state,
            "term": self.current_term,
            "voted_for": self.voted_for,
            "log_length": len(self.log),
            "commit_index": self.commit_index,
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
        votes = 1
        print(f"[Node {self.node_id}] Starting election for term {self.current_term}")

        tasks = [self._send_vote_request(peer_url) for peer_url in self.peers.values()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                continue
            if result.get("term", 0) > self.current_term:
                self._become_follower(result["term"])
                return
            if result.get("vote_granted"):
                votes += 1

        majority = (len(self.peers) + 1) // 2 + 1
        if self.state == "candidate" and votes >= majority:
            self._become_leader()

    async def _send_vote_request(self, peer_url: str) -> dict:
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index]["term"] if last_log_index >= 0 else 0
        payload = {
            "term": self.current_term,
            "candidate_id": self.node_id,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term,
        }
        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{peer_url}/request_vote", json=payload, timeout=0.1)
            return resp.json()

    # --- Vote Handler ---

    def handle_vote_request(self, term: int, candidate_id: int,
                             last_log_index: int, last_log_term: int) -> dict:
        if term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}

        if term > self.current_term:
            self._become_follower(term)

        can_vote = self.voted_for is None or self.voted_for == candidate_id
        if not can_vote:
            return {"term": self.current_term, "vote_granted": False}

        # Log up-to-date check (Raft paper §5.4.1)
        my_last_index = len(self.log) - 1
        my_last_term = self.log[my_last_index]["term"] if my_last_index >= 0 else 0
        candidate_log_ok = (
            last_log_term > my_last_term
            or (last_log_term == my_last_term and last_log_index >= my_last_index)
        )
        if not candidate_log_ok:
            return {"term": self.current_term, "vote_granted": False}

        self.voted_for = candidate_id
        self.reset_election_timer()
        print(f"[Node {self.node_id}] Voted for node {candidate_id} in term {term}")
        return {"term": self.current_term, "vote_granted": True}

    # --- Leader ---

    def _become_leader(self):
        self.state = "leader"
        print(f"[Node {self.node_id}] Became LEADER for term {self.current_term}")
        last = len(self.log) - 1
        for peer_id in self.peers:
            self.next_index[peer_id] = last + 1
            self.match_index[peer_id] = -1
        asyncio.create_task(self._send_heartbeats())

    async def _send_heartbeats(self):
        while self.state == "leader":
            tasks = [self._send_heartbeat(peer_id, peer_url) for peer_id, peer_url in self.peers.items()]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(0.05)

    async def _send_heartbeat(self, peer_id: int, peer_url: str):
        ni = self.next_index.get(peer_id, len(self.log))
        prev_index = ni - 1
        prev_term = self.log[prev_index]["term"] if prev_index >= 0 and prev_index < len(self.log) else 0
        entries = [{"term": e["term"], "command": e["command"]} for e in self.log[ni:]]
        payload = {
            "term": self.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_index,
            "prev_log_term": prev_term,
            "entries": entries,
            "leader_commit": self.commit_index,
        }
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(f"{peer_url}/append_entries", json=payload, timeout=0.1)
                data = resp.json()
            if data["term"] > self.current_term:
                self._become_follower(data["term"])
            elif data["success"]:
                self.match_index[peer_id] = prev_index + len(entries)
                self.next_index[peer_id] = self.match_index[peer_id] + 1
                self._try_commit()
            else:
                self.next_index[peer_id] = max(0, ni - 1)
        except Exception:
            pass

    # --- Log Replication ---

    def _try_commit(self):
        for n in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[n]["term"] == self.current_term:
                count = 1 + sum(1 for mid in self.match_index.values() if mid >= n)
                majority = (len(self.peers) + 1) // 2 + 1
                if count >= majority:
                    self.commit_index = n
                    self._apply_committed()
                    break

    def _apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            cmd = self.log[self.last_applied]["command"]
            self.state_machine.apply(cmd)
            print(f"[Node {self.node_id}] Applied [{self.last_applied}]: {cmd}")

    # --- AppendEntries Handler ---

    def handle_append_entries(self, term: int, leader_id: int, prev_log_index: int,
                               prev_log_term: int, entries: list, leader_commit: int) -> dict:
        if term < self.current_term:
            return {"term": self.current_term, "success": False}

        if term > self.current_term:
            self._become_follower(term)

        self.reset_election_timer()

        # Log consistency check
        if prev_log_index >= 0:
            if len(self.log) <= prev_log_index:
                return {"term": self.current_term, "success": False}
            if self.log[prev_log_index]["term"] != prev_log_term:
                self.log = self.log[:prev_log_index]
                return {"term": self.current_term, "success": False}

        # Append entries
        for i, entry in enumerate(entries):
            idx = prev_log_index + 1 + i
            if idx < len(self.log):
                if self.log[idx]["term"] != entry["term"]:
                    self.log = self.log[:idx]
                    self.log.append(entry)
            else:
                self.log.append(entry)

        # Update commit index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            self._apply_committed()

        return {"term": self.current_term, "success": True}

    # --- Client Command ---

    async def append_command(self, command: str) -> str | None:
        if self.state != "leader":
            return None
        entry = {"term": self.current_term, "command": command}
        self.log.append(entry)
        target_index = len(self.log) - 1

        for _ in range(50):  # timeout ~2.5s
            if self.commit_index >= target_index:
                # GET không thay đổi state, SET idempotent → apply lại để lấy kết quả
                return self.state_machine.apply(command)
            await asyncio.sleep(0.05)
        return "TIMEOUT"

    # --- Helpers ---

    def _become_follower(self, term: int):
        self.state = "follower"
        self.current_term = term
        self.voted_for = None
        self.reset_election_timer()
        print(f"[Node {self.node_id}] Became follower for term {term}")
