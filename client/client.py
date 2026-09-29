#!/usr/bin/env python3
"""
Raft client. Tự động redirect đến leader nếu gặp follower.

Usage:
    python client.py SET <key> <value>
    python client.py GET <key>
"""
import sys
import httpx

NODES = {
    1: "http://127.0.0.1:8001",
    2: "http://127.0.0.1:8002",
    3: "http://127.0.0.1:8003",
}


def send_command(command: str) -> str:
    tried: set[str] = set()
    current_url = NODES[1]

    for _ in range(len(NODES) + 1):
        if current_url in tried:
            break
        tried.add(current_url)

        try:
            resp = httpx.post(
                f"{current_url}/command",
                json={"command": command},
                timeout=3.0,
            )
            resp.raise_for_status()
            data = resp.json()
        except httpx.RequestError as e:
            print(f"  [client] Cannot reach {current_url}: {e}")
            remaining = [u for u in NODES.values() if u not in tried]
            if not remaining:
                break
            current_url = remaining[0]
            continue

        if data.get("success"):
            return data.get("result") or "OK"

        leader_id = data.get("leader_id")
        if leader_id and leader_id in NODES:
            print(f"  [client] Redirecting to leader node {leader_id}...")
            current_url = NODES[leader_id]
            continue

        remaining = [u for u in NODES.values() if u not in tried]
        if not remaining:
            break
        current_url = remaining[0]

    return "ERROR: could not reach the leader"


def main():
    if len(sys.argv) < 2:
        print("Usage: python client.py SET <key> <value>")
        print("       python client.py GET <key>")
        sys.exit(1)

    command = " ".join(sys.argv[1:])
    print(f"> {command}")
    result = send_command(command)
    print(f"Result: {result}")


if __name__ == "__main__":
    main()
