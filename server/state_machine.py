class StateMachine:
    def __init__(self):
        self.store: dict[str, str] = {}

    def apply(self, command: str) -> str:
        parts = command.strip().split(maxsplit=2)
        if not parts:
            return "ERR empty command"
        op = parts[0].upper()
        if op == "SET" and len(parts) == 3:
            self.store[parts[1]] = parts[2]
            return "OK"
        elif op == "GET" and len(parts) == 2:
            return self.store.get(parts[1], "(nil)")
        return "ERR unknown command"
