"""The plugin's own tables, in plain SQL that runs on PostgreSQL and SQLite.

Only numbers and short codes go in: stream ids, channel ids, account ids, counts, times, and
the short word for why a stream was moved down. No names, no URLs and nothing about who was
watching. Provider names are read live when a report is built.
"""

from __future__ import annotations

import json

PREFIX = "plugin_stream_stats_"
TABLE_NAMES = ("stream_day", "session", "rank", "rank_day", "knockdown", "rescue", "watched", "marker")

# Dispatcharr's own tables, only ever read.
CHANNELSTREAM = "dispatcharr_channels_channelstream"
STREAM = "dispatcharr_channels_stream"
CHANNEL = "dispatcharr_channels_channel"

COUNTERS = ("tuneins", "switch_ins", "fail_hard", "fail_stall", "fail_slow", "errors",
            "reconnects", "buffering", "skipped", "rescues", "seconds")


def _schema():
    counters = ", ".join(f"{name} DOUBLE PRECISION NOT NULL DEFAULT 0" for name in COUNTERS)
    return (
        f"""CREATE TABLE IF NOT EXISTS {PREFIX}stream_day (
            day TEXT NOT NULL, stream_id BIGINT NOT NULL, account_id BIGINT NOT NULL, {counters},
            PRIMARY KEY (day, stream_id))""",
        f"CREATE INDEX IF NOT EXISTS {PREFIX}stream_day_account ON {PREFIX}stream_day (account_id, day)",
        f"""CREATE TABLE IF NOT EXISTS {PREFIX}session (
            channel_uuid TEXT PRIMARY KEY, channel_id BIGINT NOT NULL, stream_id BIGINT NOT NULL,
            account_id BIGINT NOT NULL, since DOUBLE PRECISION NOT NULL, by_failover INTEGER NOT NULL)""",
        f"""CREATE TABLE IF NOT EXISTS {PREFIX}rank (
            day TEXT NOT NULL, channel_id BIGINT NOT NULL, stream_ids TEXT NOT NULL,
            PRIMARY KEY (day, channel_id))""",
        f"""CREATE TABLE IF NOT EXISTS {PREFIX}rank_day (
            day TEXT NOT NULL, account_id BIGINT NOT NULL, top1 INTEGER NOT NULL, top3 INTEGER NOT NULL,
            top5 INTEGER NOT NULL, PRIMARY KEY (day, account_id))""",
        f"""CREATE TABLE IF NOT EXISTS {PREFIX}knockdown (
            day TEXT NOT NULL, channel_id BIGINT NOT NULL, stream_id BIGINT NOT NULL, account_id BIGINT NOT NULL,
            source TEXT NOT NULL, cause TEXT, moves INTEGER NOT NULL, restored INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (day, channel_id, stream_id))""",
        f"""CREATE TABLE IF NOT EXISTS {PREFIX}rescue (
            day TEXT NOT NULL, account_id BIGINT NOT NULL, channel_id BIGINT NOT NULL,
            PRIMARY KEY (day, account_id, channel_id))""",
        f"""CREATE TABLE IF NOT EXISTS {PREFIX}watched (
            day TEXT NOT NULL, channel_id BIGINT NOT NULL, PRIMARY KEY (day, channel_id))""",
        f"""CREATE TABLE IF NOT EXISTS {PREFIX}marker (
            name TEXT PRIMARY KEY, value TEXT NOT NULL)""",
    )


class Store:
    def __init__(self, cursor_factory):
        self._cursor = cursor_factory
        self._ready = False

    def ensure(self):
        if self._ready:
            return
        for statement in _schema():
            try:
                self._exec(statement)
            except Exception:
                # Two workers can race on CREATE. A real problem shows up on the retry.
                self._exec(statement)
        self._ready = True

    def _exec(self, sql, params=()):
        with self._cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount

    def _rows(self, sql, params=()):
        with self._cursor() as cur:
            cur.execute(sql, params)
            names = [column[0] for column in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]

    # Counters

    def bump(self, day, stream_id, account_id, column, amount=1):
        if column not in COUNTERS:
            raise ValueError(column)
        self._exec(
            f"""INSERT INTO {PREFIX}stream_day (day, stream_id, account_id, {column}) VALUES (%s, %s, %s, %s)
                ON CONFLICT (day, stream_id) DO UPDATE SET
                {column} = {PREFIX}stream_day.{column} + excluded.{column},
                account_id = CASE WHEN excluded.account_id = 0 THEN {PREFIX}stream_day.account_id
                                  ELSE excluded.account_id END""",
            (day, stream_id, account_id, amount),
        )

    # Sessions

    def put_session(self, channel_uuid, channel_id, stream_id, account_id, since, by_failover):
        self._exec(
            f"""INSERT INTO {PREFIX}session (channel_uuid, channel_id, stream_id, account_id, since, by_failover)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (channel_uuid) DO UPDATE SET channel_id = excluded.channel_id,
                stream_id = excluded.stream_id, account_id = excluded.account_id,
                since = excluded.since, by_failover = excluded.by_failover""",
            (channel_uuid, channel_id, stream_id, account_id, since, int(bool(by_failover))),
        )

    def get_session(self, channel_uuid):
        rows = self._rows(f"SELECT * FROM {PREFIX}session WHERE channel_uuid = %s", (channel_uuid,))
        return rows[0] if rows else None

    def delete_session(self, channel_uuid):
        self._exec(f"DELETE FROM {PREFIX}session WHERE channel_uuid = %s", (channel_uuid,))

    # Once-a-day markers

    def claim_marker(self, name, value):
        """True for the first caller to set this value. Safe across workers."""
        return (
            self._exec(
                f"""INSERT INTO {PREFIX}marker (name, value) VALUES (%s, %s)
                    ON CONFLICT (name) DO UPDATE SET value = excluded.value
                    WHERE {PREFIX}marker.value <> excluded.value""",
                (name, value),
            )
            == 1
        )

    def get_marker(self, name):
        rows = self._rows(f"SELECT value FROM {PREFIX}marker WHERE name = %s", (name,))
        return rows[0]["value"] if rows else None

    def set_marker(self, name, value):
        self._exec(
            f"""INSERT INTO {PREFIX}marker (name, value) VALUES (%s, %s)
                ON CONFLICT (name) DO UPDATE SET value = excluded.value""",
            (name, value),
        )

    # Order snapshots

    def read_all_orders(self):
        """Every channel's order right now, from Dispatcharr's own table. Read only."""
        rows = self._rows(
            f"""SELECT cs.channel_id, cs.stream_id FROM {CHANNELSTREAM} cs
                ORDER BY cs.channel_id, cs."order", cs.id"""
        )
        orders = {}
        for row in rows:
            orders.setdefault(row["channel_id"], []).append(row["stream_id"])
        return orders

    def save_order(self, day, channel_id, stream_ids):
        self._exec(
            f"""INSERT INTO {PREFIX}rank (day, channel_id, stream_ids) VALUES (%s, %s, %s)
                ON CONFLICT (day, channel_id) DO UPDATE SET stream_ids = excluded.stream_ids""",
            (day, channel_id, json.dumps(stream_ids)),
        )

    def latest_orders_before(self, day):
        rows = self._rows(
            f"""SELECT r.channel_id, r.day, r.stream_ids FROM {PREFIX}rank r
                JOIN (SELECT channel_id, MAX(day) AS day FROM {PREFIX}rank WHERE day < %s GROUP BY channel_id) m
                ON m.channel_id = r.channel_id AND m.day = r.day""",
            (day,),
        )
        return {row["channel_id"]: (row["day"], json.loads(row["stream_ids"])) for row in rows}

    def save_rank_day(self, day, account_id, top1, top3, top5):
        self._exec(
            f"""INSERT INTO {PREFIX}rank_day (day, account_id, top1, top3, top5) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (day, account_id) DO UPDATE SET top1 = excluded.top1, top3 = excluded.top3,
                top5 = excluded.top5""",
            (day, account_id, top1, top3, top5),
        )

    def clear_rank_day(self, day):
        self._exec(f"DELETE FROM {PREFIX}rank_day WHERE day = %s", (day,))

    # Knock-downs

    def stream_accounts(self, stream_ids):
        """Which account each stream belongs to, from Dispatcharr's own table. Read only."""
        if not stream_ids:
            return {}
        marks = ", ".join(["%s"] * len(stream_ids))
        rows = self._rows(
            f"SELECT id, m3u_account_id FROM {STREAM} WHERE id IN ({marks})",
            tuple(stream_ids),
        )
        return {row["id"]: row["m3u_account_id"] or 0 for row in rows}

    def add_knockdown(self, day, channel_id, stream_id, account_id, source, cause):
        """An event counts each move. A snapshot only adds what no event already reported."""
        if source == "event":
            self._exec(
                f"""INSERT INTO {PREFIX}knockdown (day, channel_id, stream_id, account_id, source, cause, moves)
                    VALUES (%s, %s, %s, %s, 'event', %s, 1)
                    ON CONFLICT (day, channel_id, stream_id) DO UPDATE SET moves = {PREFIX}knockdown.moves + 1,
                    source = 'event', cause = excluded.cause""",
                (day, channel_id, stream_id, account_id, cause),
            )
        else:
            self._exec(
                f"""INSERT INTO {PREFIX}knockdown (day, channel_id, stream_id, account_id, source, cause, moves)
                    VALUES (%s, %s, %s, %s, 'snapshot', %s, 1)
                    ON CONFLICT (day, channel_id, stream_id) DO NOTHING""",
                (day, channel_id, stream_id, account_id, cause),
            )

    def knockdown_since(self, channel_id, stream_id, day):
        rows = self._rows(
            f"""SELECT COUNT(*) AS n FROM {PREFIX}knockdown
                WHERE channel_id = %s AND stream_id = %s AND day >= %s AND source = 'event'""",
            (channel_id, stream_id, day),
        )
        return bool(rows[0]["n"])

    def add_restore(self, day, channel_id, stream_id, account_id):
        self._exec(
            f"""INSERT INTO {PREFIX}knockdown (day, channel_id, stream_id, account_id, source, cause, moves, restored)
                VALUES (%s, %s, %s, %s, 'event', NULL, 0, 1)
                ON CONFLICT (day, channel_id, stream_id) DO UPDATE SET restored = {PREFIX}knockdown.restored + 1""",
            (day, channel_id, stream_id, account_id),
        )

    # Reports

    def provider_totals(self, first_day):
        columns = ", ".join(f"SUM({name}) AS {name}" for name in COUNTERS)
        return self._rows(
            f"SELECT account_id, {columns} FROM {PREFIX}stream_day WHERE day >= %s GROUP BY account_id",
            (first_day,),
        )

    def stream_totals(self, first_day):
        """Tries and failures per stream, for telling which streams were good."""
        return self._rows(
            f"""SELECT account_id, stream_id, SUM(tuneins + switch_ins) AS tries,
                    SUM(fail_hard + fail_stall + fail_slow) AS failures
                FROM {PREFIX}stream_day WHERE day >= %s GROUP BY account_id, stream_id""",
            (first_day,),
        )

    def knocked_streams(self, first_day):
        return self._rows(
            f"""SELECT account_id, stream_id, SUM(moves) AS moves, SUM(restored) AS restored
                FROM {PREFIX}knockdown WHERE day >= %s GROUP BY account_id, stream_id""",
            (first_day,),
        )

    def restored_counts(self, first_day):
        rows = self._rows(
            f"""SELECT account_id, COUNT(DISTINCT stream_id) AS n FROM {PREFIX}knockdown
                WHERE day >= %s AND restored > 0 GROUP BY account_id""",
            (first_day,),
        )
        return {row["account_id"]: int(row["n"]) for row in rows}

    def failure_window(self, first_day, last_day):
        rows = self._rows(
            f"""SELECT account_id, SUM(fail_hard + fail_stall + fail_slow) AS failures,
                    SUM(tuneins + switch_ins) AS tries
                FROM {PREFIX}stream_day WHERE day >= %s AND day <= %s GROUP BY account_id""",
            (first_day, last_day),
        )
        return {row["account_id"]: (float(row["failures"] or 0), float(row["tries"] or 0)) for row in rows}

    def rank_day_at_or_after(self, first_day):
        rows = self._rows(
            f"""SELECT r.account_id, r.top1 FROM {PREFIX}rank_day r
                JOIN (SELECT account_id, MIN(day) AS day FROM {PREFIX}rank_day WHERE day >= %s GROUP BY account_id) m
                ON m.account_id = r.account_id AND m.day = r.day""",
            (first_day,),
        )
        return {row["account_id"]: int(row["top1"]) for row in rows}

    def ranks_now(self):
        """Top 1, 3 and 5 per account, and the median place, from Dispatcharr's own tables."""
        return self._rows(
            f"""WITH ranked AS (
                    SELECT s.m3u_account_id AS account_id,
                           ROW_NUMBER() OVER (PARTITION BY cs.channel_id ORDER BY cs."order", cs.id) AS pos,
                           cs.channel_id
                    FROM {CHANNELSTREAM} cs JOIN {STREAM} s ON s.id = cs.stream_id
                )
                SELECT account_id,
                       COUNT(*) FILTER (WHERE pos = 1) AS top1,
                       COUNT(*) FILTER (WHERE pos <= 3) AS top3,
                       COUNT(*) FILTER (WHERE pos <= 5) AS top5
                FROM ranked WHERE account_id IS NOT NULL GROUP BY account_id"""
        )

    def positions_now(self):
        rows = self._rows(
            f"""WITH ranked AS (
                    SELECT s.m3u_account_id AS account_id,
                           ROW_NUMBER() OVER (PARTITION BY cs.channel_id ORDER BY cs."order", cs.id) AS pos
                    FROM {CHANNELSTREAM} cs JOIN {STREAM} s ON s.id = cs.stream_id
                )
                SELECT account_id, pos FROM ranked WHERE account_id IS NOT NULL ORDER BY account_id, pos"""
        )
        positions = {}
        for row in rows:
            positions.setdefault(row["account_id"], []).append(int(row["pos"]))
        return positions

    def reach_now(self):
        """Streams, streams in channels and channels served per account. Read only."""
        streams = self._rows(
            f"""SELECT m3u_account_id AS account_id, COUNT(*) AS total,
                    SUM(CASE WHEN is_stale THEN 1 ELSE 0 END) AS stale
                FROM {STREAM} WHERE m3u_account_id IS NOT NULL GROUP BY m3u_account_id"""
        )
        used = self._rows(
            f"""SELECT s.m3u_account_id AS account_id, COUNT(DISTINCT s.id) AS in_channels,
                    COUNT(DISTINCT cs.channel_id) AS channels
                FROM {CHANNELSTREAM} cs JOIN {STREAM} s ON s.id = cs.stream_id
                WHERE s.m3u_account_id IS NOT NULL GROUP BY s.m3u_account_id"""
        )
        only = self._rows(
            f"""SELECT account_id, COUNT(*) AS n FROM (
                    SELECT cs.channel_id, MIN(s.m3u_account_id) AS account_id
                    FROM {CHANNELSTREAM} cs JOIN {STREAM} s ON s.id = cs.stream_id
                    WHERE s.m3u_account_id IS NOT NULL
                    GROUP BY cs.channel_id HAVING COUNT(DISTINCT s.m3u_account_id) = 1
                ) one GROUP BY account_id"""
        )
        return streams, used, only

    def channels_total(self):
        rows = self._rows(f"SELECT COUNT(DISTINCT channel_id) AS n FROM {CHANNELSTREAM}")
        return int(rows[0]["n"])

    def add_rescue(self, day, account_id, channel_id):
        self._exec(
            f"""INSERT INTO {PREFIX}rescue (day, account_id, channel_id) VALUES (%s, %s, %s)
                ON CONFLICT (day, account_id, channel_id) DO NOTHING""",
            (day, account_id, channel_id),
        )

    def rescued_channels(self, first_day):
        rows = self._rows(
            f"""SELECT account_id, COUNT(DISTINCT channel_id) AS n FROM {PREFIX}rescue
                WHERE day >= %s GROUP BY account_id""",
            (first_day,),
        )
        return {row["account_id"]: int(row["n"]) for row in rows}

    def add_watched(self, day, channel_id):
        self._exec(
            f"""INSERT INTO {PREFIX}watched (day, channel_id) VALUES (%s, %s)
                ON CONFLICT (day, channel_id) DO NOTHING""",
            (day, channel_id),
        )

    def channels_watched(self, first_day):
        rows = self._rows(
            f"SELECT COUNT(DISTINCT channel_id) AS n FROM {PREFIX}watched WHERE day >= %s",
            (first_day,),
        )
        return int(rows[0]["n"])

    def watched_by_account(self, first_day):
        """How many of the channels people watched have a stream from each account."""
        rows = self._rows(
            f"""SELECT s.m3u_account_id AS account_id, COUNT(DISTINCT w.channel_id) AS n
                FROM {PREFIX}watched w
                JOIN {CHANNELSTREAM} cs ON cs.channel_id = w.channel_id
                JOIN {STREAM} s ON s.id = cs.stream_id
                WHERE w.day >= %s AND s.m3u_account_id IS NOT NULL
                GROUP BY s.m3u_account_id""",
            (first_day,),
        )
        return {row["account_id"]: int(row["n"]) for row in rows}

    # Housekeeping

    def prune(self, first_day, first_order_day):
        for table in ("stream_day", "rank_day", "knockdown", "rescue", "watched"):
            self._exec(f"DELETE FROM {PREFIX}{table} WHERE day < %s", (first_day,))
        # Full orders are only needed for comparing with the next one, and there can be many.
        self._exec(f"DELETE FROM {PREFIX}rank WHERE day < %s", (first_order_day,))

    def erase(self):
        for table in TABLE_NAMES:
            self._exec(f"DELETE FROM {PREFIX}{table}")
