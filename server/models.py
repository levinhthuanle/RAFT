from pydantic import BaseModel


class VoteRequest(BaseModel):
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


class VoteResponse(BaseModel):
    term: int
    vote_granted: bool


class LogEntry(BaseModel):
    term: int
    command: str


class AppendEntriesRequest(BaseModel):
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int


class AppendEntriesResponse(BaseModel):
    term: int
    success: bool


class ClientCommand(BaseModel):
    command: str


class ClientResponse(BaseModel):
    success: bool
    result: str | None = None
    leader_id: int | None = None
