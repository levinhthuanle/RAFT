class RaftNode:
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers

        # Raft state
        self.state = "follower"

        self.current_term = 0
        self.voted_for = None

        # Chưa dùng đến, nhưng khai báo từ đầu
        self.log = []
        self.commit_index = -1

    def get_status(self):
        return {
            "node_id": self.node_id,
            "state": self.state,
            "term": self.current_term,
            "voted_for": self.voted_for,
        }