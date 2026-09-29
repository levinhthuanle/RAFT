import asyncio
import json
import os
import pathlib
import random
import httpx
from state_machine import StateMachine

SNAPSHOT_THRESHOLD = 50


class RaftNode:
    def __str__(self):
        return f"RaftNode(id={self.node_id}, state={self.state}, term={self.current_term}, peers={list(self.peers.keys())})"

    def __init__(self, node_id, peers, data_dir="data", database_url=None):
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

        self.state_machine = StateMachine(database_url=database_url)

        self._loop = asyncio.get_event_loop()
        self.last_heartbeat = self._loop.time()

        self._state_path = pathlib.Path(data_dir) / f"node_{node_id}.json"
        self._snapshot_path = pathlib.Path(data_dir) / f"snapshot_{node_id}.json"
        self._state_path.parent.mkdir(parents=True, exist_ok=True)

        self.snapshot_last_index = -1
        self.snapshot_last_term = 0

        self._load_snapshot()
        self._load_persistent_state()

    # --- Index helpers (log bị truncate nên index thật ≠ vị trí trong list) ---

    def _log_len(self) -> int:
        return self.snapshot_last_index + 1 + len(self.log)

    def _log_pos(self, index: int) -> int:
        return index - (self.snapshot_last_index + 1)

    def _log_term_at(self, index: int) -> int:
        if index < 0:
            return 0
        if index == self.snapshot_last_index:
            return self.snapshot_last_term
        if index < self.snapshot_last_index:
            return 0
        pos = self._log_pos(index)
        if 0 <= pos < len(self.log):
            return self.log[pos]["term"]
        return 0

    def get_status(self):
        return {
            "node_id": self.node_id,
            "state": self.state,
            "term": self.current_term,
            "voted_for": self.voted_for,
            "log_length": self._log_len(),
            "commit_index": self.commit_index,
            "snapshot_last_index": self.snapshot_last_index,
        }

    def reset_election_timer(self):
        self.last_heartbeat = self._loop.time()

    # --- Persistence ---

    def _load_snapshot(self):
        if not self._snapshot_path.exists():
            return
        with open(self._snapshot_path) as f:
            data = json.load(f)
        self.snapshot_last_index = data["last_included_index"]
        self.snapshot_last_term = data["last_included_term"]
        self.state_machine.restore_snapshot(data["store"])
        self.last_applied = self.snapshot_last_index
        self.commit_index = self.snapshot_last_index
        print(f"[Node {self.node_id}] Restored snapshot at index {self.snapshot_last_index}")

    def _load_persistent_state(self):
        if not self._state_path.exists():
            return
        with open(self._state_path) as f:
            data = json.load(f)
        self.current_term = data.get("current_term", 0)
        self.voted_for = data.get("voted_for", None)
        self.log = data.get("log", [])
        # Replay chỉ phần log chưa có trong snapshot, và chỉ khi dùng in-memory store
        if not self.state_machine._conn:
            for entry in self.log:
                self.state_machine.apply(entry["command"])
        self.last_applied = self._log_len() - 1
        print(f"[Node {self.node_id}] Loaded state: term={self.current_term}, log_len={self._log_len()}")

    def _save_persistent_state(self):
        data = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": self.log,
        }
        tmp = str(self._state_path) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._state_path)

    def _save_snapshot(self):
        if self.commit_index < self.snapshot_last_index + SNAPSHOT_THRESHOLD:
            return
        pos = self._log_pos(self.commit_index)
        if pos < 0 or pos >= len(self.log):
            return
        data = {
            "last_included_index": self.commit_index,
            "last_included_term": self.log[pos]["term"],
            "store": self.state_machine.get_snapshot(),
        }
        tmp = str(self._snapshot_path) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._snapshot_path)
        # Truncate log — chỉ giữ entries SAU snapshot
        self.log = self.log[pos + 1:]
        self.snapshot_last_index = data["last_included_index"]
        self.snapshot_last_term = data["last_included_term"]
        self._save_persistent_state()
        print(f"[Node {self.node_id}] Snapshot at index {self.snapshot_last_index}, log trimmed to {len(self.log)} entries")

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
        self._save_persistent_state()
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
        last_log_index = self._log_len() - 1
        last_log_term = self._log_term_at(last_log_index)
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

        my_last_index = self._log_len() - 1
        my_last_term = self._log_term_at(my_last_index)
        candidate_log_ok = (
            last_log_term > my_last_term
            or (last_log_term == my_last_term and last_log_index >= my_last_index)
        )
        if not candidate_log_ok:
            return {"term": self.current_term, "vote_granted": False}

        self.voted_for = candidate_id
        self._save_persistent_state()
        self.reset_election_timer()
        print(f"[Node {self.node_id}] Voted for node {candidate_id} in term {term}")
        return {"term": self.current_term, "vote_granted": True}

    # --- Leader ---

    def _become_leader(self):
        self.state = "leader"
        print(f"[Node {self.node_id}] Became LEADER for term {self.current_term}")
        last = self._log_len() - 1
        for peer_id in self.peers:
            self.next_index[peer_id] = last + 1
            self.match_index[peer_id] = -1
        self.log.append({"term": self.current_term, "command": "NOOP"})
        self._save_persistent_state()
        asyncio.create_task(self._send_heartbeats())

    async def _send_heartbeats(self):
        while self.state == "leader":
            tasks = [self._send_heartbeat(peer_id, peer_url) for peer_id, peer_url in self.peers.items()]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(0.05)

    async def _send_heartbeat(self, peer_id: int, peer_url: str):
        ni = self.next_index.get(peer_id, self._log_len())

        # Peer lag quá xa → gửi snapshot thay vì AppendEntries
        if ni <= self.snapshot_last_index:
            await self._send_install_snapshot(peer_id, peer_url)
            return

        prev_index = ni - 1
        prev_term = self._log_term_at(prev_index)
        entries = [{"term": e["term"], "command": e["command"]} for e in self.log[self._log_pos(ni):]]
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
                self.next_index[peer_id] = max(self.snapshot_last_index + 1, ni - 1)
        except Exception:
            pass

    async def _send_install_snapshot(self, peer_id: int, peer_url: str):
        if not self._snapshot_path.exists():
            return
        with open(self._snapshot_path) as f:
            snap = json.load(f)
        payload = {
            "term": self.current_term,
            "leader_id": self.node_id,
            "last_included_index": snap["last_included_index"],
            "last_included_term": snap["last_included_term"],
            "store": snap["store"],
        }
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(f"{peer_url}/install_snapshot", json=payload, timeout=2.0)
                data = resp.json()
            if data.get("term", 0) > self.current_term:
                self._become_follower(data["term"])
            else:
                self.match_index[peer_id] = snap["last_included_index"]
                self.next_index[peer_id] = snap["last_included_index"] + 1
        except Exception:
            pass

    # --- Log Replication ---

    def _try_commit(self):
        for n in range(self._log_len() - 1, self.commit_index, -1):
            if self._log_term_at(n) == self.current_term:
                count = 1 + sum(1 for mid in self.match_index.values() if mid >= n)
                majority = (len(self.peers) + 1) // 2 + 1
                if count >= majority:
                    self.commit_index = n
                    self._apply_committed()
                    break

    def _apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            pos = self._log_pos(self.last_applied)
            cmd = self.log[pos]["command"]
            self.state_machine.apply(cmd)
            print(f"[Node {self.node_id}] Applied [{self.last_applied}]: {cmd}")
        self._save_snapshot()

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
            if prev_log_index > self.snapshot_last_index:
                if self._log_len() <= prev_log_index:
                    return {"term": self.current_term, "success": False}
                if self._log_term_at(prev_log_index) != prev_log_term:
                    # Truncate đến prev_log_index
                    self.log = self.log[:self._log_pos(prev_log_index)]
                    self._save_persistent_state()
                    return {"term": self.current_term, "success": False}
            elif prev_log_index < self.snapshot_last_index:
                # Entry đã được snapshot — không thể kiểm tra, reject để leader retry
                return {"term": self.current_term, "success": False}
            # prev_log_index == snapshot_last_index: term đã được lưu trong snapshot → ok

        # Append entries
        for i, entry in enumerate(entries):
            idx = prev_log_index + 1 + i
            if idx <= self.snapshot_last_index:
                continue  # đã có trong snapshot, bỏ qua
            pos = self._log_pos(idx)
            if pos < len(self.log):
                if self.log[pos]["term"] != entry["term"]:
                    self.log = self.log[:pos]
                    self.log.append(entry)
            else:
                self.log.append(entry)

        if entries:
            self._save_persistent_state()

        # Update commit index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self._log_len() - 1)
            self._apply_committed()

        return {"term": self.current_term, "success": True}

    # --- InstallSnapshot Handler ---

    def handle_install_snapshot(self, term: int, leader_id: int,
                                  last_included_index: int, last_included_term: int,
                                  store: dict) -> dict:
        if term < self.current_term:
            return {"term": self.current_term}
        if term > self.current_term:
            self._become_follower(term)
        self.reset_election_timer()

        if last_included_index <= self.snapshot_last_index:
            return {"term": self.current_term}

        self.state_machine.restore_snapshot(store)

        # Giữ lại phần log sau snapshot nếu còn
        retain_pos = self._log_pos(last_included_index + 1)
        self.log = self.log[retain_pos:] if retain_pos >= 0 else []

        self.snapshot_last_index = last_included_index
        self.snapshot_last_term = last_included_term
        self.commit_index = last_included_index
        self.last_applied = last_included_index

        data = {
            "last_included_index": last_included_index,
            "last_included_term": last_included_term,
            "store": store,
        }
        tmp = str(self._snapshot_path) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._snapshot_path)
        self._save_persistent_state()
        print(f"[Node {self.node_id}] Installed snapshot at index {last_included_index}")
        return {"term": self.current_term}

    # --- Client Command ---

    async def append_command(self, command: str) -> str | None:
        if self.state != "leader":
            return None
        entry = {"term": self.current_term, "command": command}
        self.log.append(entry)
        target_index = self._log_len() - 1

        for _ in range(50):
            if self.commit_index >= target_index:
                return self.state_machine.apply(command)
            await asyncio.sleep(0.05)
        return "TIMEOUT"

    # --- Helpers ---

    def _become_follower(self, term: int):
        self.state = "follower"
        self.current_term = term
        self.voted_for = None
        self._save_persistent_state()
        self.reset_election_timer()
        print(f"[Node {self.node_id}] Became follower for term {term}")
