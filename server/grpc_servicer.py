import raft_pb2
import raft_pb2_grpc


class RaftServicer(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node):
        self.node = node

    def RequestVote(self, request, context):
        r = self.node.handle_vote_request(
            request.term, request.candidate_id,
            request.last_log_index, request.last_log_term,
        )
        return raft_pb2.VoteResponse(term=r["term"], vote_granted=r["vote_granted"])

    def AppendEntries(self, request, context):
        entries = [{"term": e.term, "command": e.command} for e in request.entries]
        r = self.node.handle_append_entries(
            request.term, request.leader_id,
            request.prev_log_index, request.prev_log_term,
            entries, request.leader_commit,
        )
        return raft_pb2.AppendEntriesResponse(term=r["term"], success=r["success"])

    def InstallSnapshot(self, request, context):
        store = dict(request.store)
        r = self.node.handle_install_snapshot(
            request.term, request.leader_id,
            request.last_included_index, request.last_included_term,
            store,
        )
        return raft_pb2.InstallSnapshotResponse(term=r["term"])
