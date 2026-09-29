import psycopg2
import psycopg2.extras


class StateMachine:
    def __init__(self, database_url: str | None = None):
        self._db_url = database_url
        self._conn = None
        self.store: dict[str, str] = {}
        if database_url:
            self._connect()

    def _connect(self):
        self._conn = psycopg2.connect(self._db_url)
        self._conn.autocommit = True
        with self._conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kv (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)

    def apply(self, command: str) -> str:
        parts = command.strip().split(maxsplit=2)
        if not parts:
            return "ERR empty command"
        op = parts[0].upper()
        if op == "NOOP":
            return "OK"
        if op == "SET" and len(parts) == 3:
            return self._set(parts[1], parts[2])
        elif op == "GET" and len(parts) == 2:
            return self._get(parts[1])
        return "ERR unknown command"

    def _set(self, key: str, value: str) -> str:
        if self._conn:
            with self._conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO kv (key, value) VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """, (key, value))
        else:
            self.store[key] = value
        return "OK"

    def _get(self, key: str) -> str:
        if self._conn:
            with self._conn.cursor() as cur:
                cur.execute("SELECT value FROM kv WHERE key = %s", (key,))
                row = cur.fetchone()
                return row[0] if row else "(nil)"
        return self.store.get(key, "(nil)")

    def get_snapshot(self) -> dict:
        if self._conn:
            with self._conn.cursor() as cur:
                cur.execute("SELECT key, value FROM kv")
                return dict(cur.fetchall())
        return dict(self.store)

    def restore_snapshot(self, store: dict):
        if self._conn:
            with self._conn.cursor() as cur:
                cur.execute("TRUNCATE kv")
                if store:
                    psycopg2.extras.execute_values(
                        cur, "INSERT INTO kv (key, value) VALUES %s",
                        list(store.items())
                    )
        else:
            self.store = dict(store)
