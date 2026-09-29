# RAFT Project — TODO

## Done
- [x] Leader election + heartbeat
- [x] Log replication (AppendEntries với consistency check)
- [x] State machine — Postgres key-value store (SET/GET)
- [x] Persistence — JSON file atomic (fsync + os.replace)
- [x] Log up-to-date check khi vote (§5.4.1)
- [x] No-op entry khi leader mới được bầu
- [x] Log compaction / Snapshot + InstallSnapshot RPC
- [x] Docker Compose: 3 nodes + 3 Postgres DB
- [x] Client redirect đến leader tự động

## Todo

### High priority
- [ ] **gRPC** — đổi internal RPC (RequestVote, AppendEntries, InstallSnapshot) từ HTTP sang gRPC. Client vẫn dùng HTTP.
- [ ] **Prometheus + Grafana** — metrics: election count, commit latency, replication lag, snapshot frequency.

### Medium priority
- [ ] **Visualize** — dashboard realtime: node nào là leader, log length từng node, state cluster.
- [ ] **Read index** — follower serve read với strong consistency, kết hợp nginx LB để scale read.
- [ ] **Membership change** — thêm/xóa node khi cluster đang chạy mà không cần down.

### Nice to have
- [ ] **Mở rộng state machine** — `DEL`, `INCR`, `EXISTS`, TTL, transactions.
