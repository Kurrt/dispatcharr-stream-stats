"""What the tests share: real SQLite, stand-ins for Dispatcharr's tables, and a fake clock."""

import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from stream_stats import engine  # noqa: E402
from stream_stats.service import Service  # noqa: E402
from stream_stats.store import CHANNELSTREAM, STREAM, Store  # noqa: E402


class _Cursor:
    def __init__(self, conn):
        self._cur = conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self._cur.close()
        return False

    def execute(self, sql, params=()):
        return self._cur.execute(sql.replace("%s", "?"), tuple(params))

    def __getattr__(self, name):
        return getattr(self._cur, name)


class Clock:
    def __init__(self, now=1_800_000_000.0):
        self.now = now

    def __call__(self):
        return self.now


class FakeDispatcharr:
    def __init__(self, world):
        self.world = world
        self.enabled = True
        self.settings = {}
        self.names = {}
        self.resolution_text = {}
        self.redirects = 0
        # Channels whose data stopped, as when, and channels Dispatcharr has forgotten about.
        self.last_data = {}

    def plugin_state(self):
        return self.enabled, self.settings

    def channel_id(self, channel_uuid):
        return self.world.uuids.get(str(channel_uuid))

    def last_data_at(self, channel_uuid):
        return self.last_data.get(str(channel_uuid), self.world.clock.now)

    def stream_account(self, stream_id):
        rows = self.world.conn.execute(f"SELECT m3u_account_id FROM {STREAM} WHERE id = ?", (stream_id,)).fetchall()
        return rows[0][0] if rows else None

    def providers(self):
        return dict(self.names)

    def redirect_channels(self):
        return self.redirects

    def resolutions(self):
        return dict(self.resolution_text)


class World:
    def __init__(self, settings=None):
        self.conn = sqlite3.connect(":memory:", isolation_level=None)
        self.conn.execute(f"""CREATE TABLE {STREAM} (id INTEGER PRIMARY KEY, m3u_account_id INTEGER, is_stale INTEGER DEFAULT 0)""")
        self.conn.execute(f"""CREATE TABLE {CHANNELSTREAM} (id INTEGER PRIMARY KEY AUTOINCREMENT, channel_id INTEGER, stream_id INTEGER, "order" INTEGER)""")
        self.clock = Clock()
        self.uuids = {}
        self.dp = FakeDispatcharr(self)
        self.dp.settings.update(settings or {})
        self.store = Store(lambda: _Cursor(self.conn))
        self.service = Service(self.store, self.dp, clock=self.clock)
        self.store.ensure()

    def __del__(self):
        self.conn.close()

    def provider(self, account_id, name, limit=0):
        self.dp.names[account_id] = {"name": name, "limit": limit}

    def stream(self, stream_id, account_id, stale=0):
        self.conn.execute(f"INSERT INTO {STREAM} (id, m3u_account_id, is_stale) VALUES (?, ?, ?)", (stream_id, account_id, stale))

    def channel(self, channel_id, stream_ids):
        uuid = f"00000000-0000-0000-0000-{channel_id:012d}"
        self.uuids[uuid] = channel_id
        for position, stream_id in enumerate(stream_ids):
            self.conn.execute(f"""INSERT INTO {CHANNELSTREAM} (channel_id, stream_id, "order") VALUES (?, ?, ?)""", (channel_id, stream_id, position))
        return uuid

    def set_order(self, channel_id, stream_ids):
        for position, stream_id in enumerate(stream_ids):
            self.conn.execute(f"""UPDATE {CHANNELSTREAM} SET "order" = ? WHERE channel_id = ? AND stream_id = ?""", (position, channel_id, stream_id))

    def uuid(self, channel_id):
        return next(u for u, c in self.uuids.items() if c == channel_id)

    def tick(self, seconds):
        self.clock.now += seconds

    def send(self, event, channel_id=None, **payload):
        if channel_id is not None:
            payload["channel_id"] = self.uuid(channel_id)
        self.service.handle_event(event, payload)

    def start(self, channel_id, stream_id):
        self.send("channel_start", channel_id, stream_id=stream_id)

    def fail(self, channel_id, previous, new, reason="max_retries_exceeded"):
        self.send("stream_switch", channel_id, previous_stream_id=previous, stream_id=new, reason=reason)

    def stop(self, channel_id):
        self.send("channel_stop", channel_id, runtime=1.0)

    def report_row(self, account_id):
        report = self.service.build_report()
        return next(r for r in report["providers"] if r["account_id"] == account_id)

    def dump_all_tables(self):
        text = []
        for table, in self.conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'plugin_%'").fetchall():
            for row in self.conn.execute(f"SELECT * FROM {table}").fetchall():
                text.append(repr(row))
        return "\n".join(text)


__all__ = ["World", "engine", "ROOT"]
