from pydantic import BaseModel


class VoteRequest(BaseModel):
    term: int
    candidate_id: int


class VoteResponse(BaseModel):
    term: int
    vote_granted: bool


class AppendEntriesRequest(BaseModel):
    term: int
    leader_id: int


class AppendEntriesResponse(BaseModel):
    term: int
    success: bool
