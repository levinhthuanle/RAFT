# Raft — Giải thích toàn bộ

## 1. Vấn đề Raft giải quyết

Bạn có một database. Nếu server chết, data mất. Giải pháp: chạy nhiều bản sao (replica) của database đó trên nhiều máy khác nhau.

Nhưng ngay lập tức xuất hiện câu hỏi: **khi client ghi data vào, làm sao đảm bảo tất cả các bản sao đều có cùng data, theo đúng thứ tự?**

Raft là thuật toán giải quyết đúng vấn đề này. Nó đảm bảo:
- Tất cả node đều apply các lệnh theo **cùng một thứ tự**
- Không bao giờ mất data đã được xác nhận với client
- Cluster vẫn hoạt động khi có tối đa `(n-1)/2` node chết

---

## 2. Khái niệm cốt lõi

### Term — "nhiệm kỳ"

Thời gian trong Raft được chia thành các **term** — mỗi term là một khoảng thời gian có đúng một leader. Term tăng dần, không bao giờ giảm.

```
Term 1        Term 2       Term 3
|──────────|  |────────|   |──────────────
Leader: N1    Leader: N2   Leader: N1
```

Term đóng vai trò như **đồng hồ logic** — nếu một node thấy term cao hơn của mình, nó biết ngay mình đang bị lỗi thời và tự về follower.

### Log — "nhật ký lệnh"

Mỗi node giữ một **log** — danh sách các lệnh theo thứ tự:

```
index:  0                1                2
       {"term":1,        {"term":1,        {"term":2,
        "command":        "command":        "command":
        "SET x 1"}        "SET y 2"}        "SET x 99"}
```

Log là **source of truth**. Database (Postgres) chỉ là kết quả của việc replay log từ đầu đến cuối. Nếu hai node có log giống hệt nhau, database của chúng sẽ giống hệt nhau.

### State machine

Là thứ nhận log entry và thực thi nó. Trong project này là Postgres:

```
log entry "SET foo bar"  →  state machine  →  INSERT INTO kv VALUES ('foo','bar')
log entry "SET foo baz"  →  state machine  →  UPDATE kv SET value='baz' WHERE key='foo'
```

---

## 3. Ba trạng thái của một node

Mỗi node tại bất kỳ thời điểm nào đều là một trong ba:

```
        timeout             thắng election
Follower ──────► Candidate ──────────────► Leader
    ▲                │                        │
    │                │ thua election           │ heartbeat
    └────────────────┘                        │
    ▲                                         │
    └─────────────────────────────────────────┘
              nhận heartbeat / term cao hơn
```

- **Follower**: thụ động, chỉ nhận và trả lời RPC. Không tự làm gì.
- **Candidate**: đang cố gắng trở thành leader, đi xin phiếu.
- **Leader**: duy nhất được nhận lệnh từ client và replicate sang follower.

---

## 4. Leader Election — bầu chọn leader

### Election timer

Mỗi follower giữ một **election timer**. Nếu không nhận được heartbeat từ leader trong khoảng `[150ms, 300ms]` (random để tránh tất cả cùng timeout một lúc), nó kết luận "leader đã chết" và bắt đầu election.

```python
# raft.py:148
async def run_election_timer(self):
    while True:
        timeout = random.uniform(0.15, 0.3)   # random để tránh split vote
        await asyncio.sleep(timeout)
        if self.state == "leader":
            continue
        elapsed = self._loop.time() - self.last_heartbeat
        if elapsed >= timeout:
            await self.start_election()
```

### Quá trình bầu

1. Node tự chuyển thành `candidate`, tăng `current_term`, tự vote cho mình
2. Gửi `RequestVote` song song đến tất cả peer
3. Nếu nhận được đa số phiếu (`≥ n/2 + 1`) → trở thành leader
4. Nếu thua hoặc timeout → về follower, chờ election tiếp

```python
# raft.py:162
async def start_election(self):
    self.state = "candidate"
    self.current_term += 1
    self.voted_for = self.node_id
    self._save_persistent_state()   # phải lưu trước khi làm bất cứ gì
    votes = 1                       # tự vote cho mình
    ...
    majority = (len(self.peers) + 1) // 2 + 1
    if self.state == "candidate" and votes >= majority:
        self._become_leader()
```

### Điều kiện vote (§5.4.1)

Một node chỉ vote cho candidate nếu thỏa cả hai:
1. Chưa vote cho ai trong term này (hoặc đã vote cho chính candidate đó)
2. **Log của candidate phải "mới hơn hoặc bằng" log của mình** — đây là rule quan trọng nhất để đảm bảo safety

"Mới hơn" được định nghĩa: term cuối cao hơn, hoặc cùng term thì log dài hơn.

```python
# raft.py:215
candidate_log_ok = (
    last_log_term > my_last_term
    or (last_log_term == my_last_term and last_log_index >= my_last_index)
)
```

**Tại sao cần điều này?** Nếu không kiểm tra, một node bị lag (mất nhiều entry) vẫn có thể được bầu làm leader và ghi đè data đã committed của cluster.

---

## 5. Log Replication — nhân bản dữ liệu

### Heartbeat

Leader gửi `AppendEntries` đến tất cả follower **mỗi 50ms**. Khi không có entry mới thì gọi là heartbeat — chỉ để báo "tôi vẫn sống".

### Khi có lệnh mới từ client

```
1. Client gửi "SET foo bar" đến leader
2. Leader append vào log của mình: {"term": 2, "command": "SET foo bar"}
3. Heartbeat kế tiếp gửi entry này đến tất cả follower
4. Follower append vào log, trả success
5. Leader nhận đủ majority → commit
6. Leader apply vào state machine (Postgres)
7. Leader trả kết quả về client
8. Heartbeat kế tiếp mang leader_commit mới → follower cũng apply vào Postgres
```

### Consistency check

Trước khi follower append entries mới, nó kiểm tra: "entry ngay trước đó trong log của tôi có khớp với leader không?"

```
Leader log:    [A][B][C][D]
                        ▲
                   prev_log_index=2, prev_log_term=?

Follower log:  [A][B][X]   ← index 2 khác term → reject
```

Nếu không khớp → follower reject, leader backup `next_index` một bước rồi retry. Cứ thế cho đến khi tìm được điểm chung, rồi ghi đè từ đó về sau.

```python
# raft.py:339
if prev_log_index >= 0:
    if self._log_term_at(prev_log_index) != prev_log_term:
        self.log = self.log[:self._log_pos(prev_log_index)]
        return {"term": self.current_term, "success": False}
```

### Commit rule (§5.4.2)

Leader **chỉ commit entry của current term** trực tiếp:

```python
# raft.py:309
if self._log_term_at(n) == self.current_term:  # chỉ commit của term hiện tại
    count = 1 + sum(1 for mid in self.match_index.values() if mid >= n)
    if count >= majority:
        self.commit_index = n
```

Khi một entry của current term được commit, tất cả entries cũ phía trước cũng được commit theo. Điều này tránh "Figure 8 problem" trong Raft paper — trường hợp leader cũ bị thay thế rồi data bị ghi đè.

---

## 6. No-op entry — vấn đề khi leader mới được bầu

Khi leader mới được bầu, nó biết log của mình nhưng không biết entry nào đã thực sự được committed (vì `commit_index` bị reset về -1 sau restart, hoặc leader cũ chết trước khi broadcast `leader_commit`).

Nếu client đọc ngay lúc này, có thể nhận được data cũ.

**Giải pháp:** leader mới **ngay lập tức append một entry NOOP** vào log:

```python
# raft.py:237
self.log.append({"term": self.current_term, "command": "NOOP"})
```

NOOP phải được majority ack trước khi leader serve bất kỳ client nào. Khi NOOP được commit, `_try_commit` sẽ advance `commit_index` qua toàn bộ log cũ — tất cả entries phía trước cũng được apply vào Postgres.

`StateMachine.apply("NOOP")` chỉ return `"OK"` mà không làm gì với database.

---

## 7. Persistence — bền vững qua restart

Raft paper §5.8 yêu cầu **3 trường phải được ghi ra disk trước khi respond bất kỳ RPC nào**:

| Trường | Lý do |
|---|---|
| `current_term` | Không được vote 2 lần trong cùng một term |
| `voted_for` | Không được vote 2 lần trong cùng một term |
| `log` | Không được mất entry đã commit |

Project này lưu vào file JSON, ghi **atomic** bằng cách write vào file `.tmp` → `fsync` → `os.replace`:

```python
# raft.py:109
def _save_persistent_state(self):
    tmp = str(self._state_path) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
        f.flush()
        os.fsync(f.fileno())   # đảm bảo data xuống disk thật sự
    os.replace(tmp, self._state_path)   # atomic rename
```

`os.replace` là atomic trên Linux/Mac — không bao giờ có trạng thái file bị hỏng giữa chừng.

**Những thứ KHÔNG cần persist:** `commit_index`, `last_applied`, `next_index`, `match_index` — vì chúng đều có thể rebuild từ log và từ communication với leader sau restart.

---

## 8. Log Compaction — snapshot

Log tăng mãi không dừng. Sau 1 triệu lệnh, restart sẽ phải replay 1 triệu entry.

**Giải pháp:** định kỳ chụp **snapshot** — ghi toàn bộ state machine tại một thời điểm, rồi xóa log cũ.

```
Trước snapshot:
log:  [SET x 1][SET y 2][SET x 3][SET z 4]  ← 4 entries, tốn RAM
                                    ▲
                             commit_index = 3

Sau snapshot tại index 3:
snapshot: {x:3, y:2, z:4}   ← ghi ra disk
log:  []                     ← truncate hoàn toàn
snapshot_last_index = 3
```

Trong project này snapshot được chụp mỗi khi commit thêm `SNAPSHOT_THRESHOLD = 50` entries:

```python
# raft.py:122
def _save_snapshot(self):
    if self.commit_index < self.snapshot_last_index + SNAPSHOT_THRESHOLD:
        return
    ...
    self.log = self.log[pos + 1:]   # truncate
```

### Index helpers sau truncate

Sau khi log bị truncate, `self.log[0]` không còn là index 0 thật nữa. Cần 3 helper:

```python
# logical length của log = snapshot đã gộp + log còn lại
def _log_len(self):
    return self.snapshot_last_index + 1 + len(self.log)

# vị trí trong list self.log của một absolute index
def _log_pos(self, index):
    return index - (self.snapshot_last_index + 1)

# term tại một absolute index (có thể nằm trong snapshot)
def _log_term_at(self, index):
    if index == self.snapshot_last_index:
        return self.snapshot_last_term
    pos = self._log_pos(index)
    return self.log[pos]["term"] if 0 <= pos < len(self.log) else 0
```

### InstallSnapshot — cho follower lag quá xa

Nếu follower cần entries đã bị truncate khỏi log của leader, leader không thể gửi AppendEntries nữa. Thay vào đó leader gửi toàn bộ snapshot:

```python
# raft.py:247
if ni <= self.snapshot_last_index:
    await self._send_install_snapshot(peer_id, peer_url)
    return
```

Follower nhận snapshot → restore Postgres từ snapshot → cập nhật `commit_index`, `last_applied`, `snapshot_last_index` → sẵn sàng nhận entries mới.

---

## 9. Các RPC và flow đầy đủ

### Ba RPC nội bộ giữa các node

```
POST /request_vote      candidate → peers
  Request:  {term, candidate_id, last_log_index, last_log_term}
  Response: {term, vote_granted}

POST /append_entries    leader → followers, mỗi 50ms
  Request:  {term, leader_id, prev_log_index, prev_log_term,
             entries[], leader_commit}
  Response: {term, success}

POST /install_snapshot  leader → follower lag quá xa
  Request:  {term, leader_id, last_included_index,
             last_included_term, store{}}
  Response: {term}
```

### Flow một write request từ đầu đến cuối

```
Client: POST /command {"command": "SET foo bar"}
  │
  ▼
Leader (node1):
  append {"term":2, "command":"SET foo bar"} vào log[5]
  save node_1.json
  │
  ├── POST /append_entries → node2 (prev_log_index=4, entries=[log[5]])
  │       node2: consistency check ok → append → save → return success
  │
  ├── POST /append_entries → node3
  │       node3: consistency check ok → append → save → return success
  │
  ▼ (nhận success từ node2, tổng = 2/3 = majority)
  _try_commit(): commit_index = 5
  _apply_committed(): state_machine.apply("SET foo bar") → Postgres DB1
  │
  ▼ heartbeat kế tiếp (50ms sau) mang leader_commit=5
  ├── node2 nhận leader_commit=5 → apply → Postgres DB2
  └── node3 nhận leader_commit=5 → apply → Postgres DB3
  │
  ▼
Client nhận: {"success": true, "result": "OK"}
```

---

## 10. Safety guarantees

Raft đảm bảo hai điều tuyệt đối:

**1. Election Safety:** tại mỗi term, tối đa một leader được bầu.
- Đảm bảo bởi: mỗi node chỉ vote một lần mỗi term, majority vote không thể trao cho hai node khác nhau.

**2. Log Matching:** nếu hai node có entry cùng index và cùng term, thì toàn bộ log từ đầu đến đó là giống hệt nhau.
- Đảm bảo bởi: consistency check trong AppendEntries — leader verify điểm kết nối trước khi append.

**3. Leader Completeness:** một entry đã committed sẽ xuất hiện trong log của tất cả leader tương lai.
- Đảm bảo bởi: log up-to-date check khi vote — node có log cũ hơn không bao giờ được bầu làm leader.

---

## 11. Cấu trúc project này

```
server/
├── main.py          — entry point, parse args, khởi động uvicorn
├── app.py           — FastAPI, định nghĩa endpoints, tạo RaftNode
├── raft.py          — toàn bộ logic Raft (RaftNode class)
├── state_machine.py — Postgres key-value store, apply(command)
├── models.py        — Pydantic models cho tất cả RPC
└── const.py         — đọc PEERS từ env, fallback về 127.0.0.1

client/
└── client.py        — gửi lệnh, tự redirect đến leader

script/
└── start_cluster.sh — khởi động cluster local (không dùng Docker)

Dockerfile           — build image cho node
docker-compose.yml   — 3 nodes + 3 Postgres DB
```

### Dữ liệu lưu trên disk (mỗi node)

```
/data/
├── node_N.json       — {current_term, voted_for, log[]}
└── snapshot_N.json   — {last_included_index, last_included_term, store{}}
```

---

## 12. Những thứ chưa implement

| Tính năng | Ý nghĩa |
|---|---|
| Membership change | Thêm/xóa node khi đang chạy |
| Linearizable read | Read từ follower mà vẫn strong consistency |
| Pipeline replication | Gửi nhiều batch song song thay vì tuần tự |
| gRPC | Binary protocol, thay HTTP để giảm latency |
| Observability | Metrics, dashboard theo dõi cluster |
