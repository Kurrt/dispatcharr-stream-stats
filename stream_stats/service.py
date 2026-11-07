"""Turns Dispatcharr's events and stream order into per-provider numbers. Writes only its own tables."""

from __future__ import annotations

import csv
import html
import io
import json
import logging
import os
import statistics
import time

from . import engine

logger = logging.getLogger(__name__)

DUPLICATE_SECONDS = 5
FALSE_RECONNECT_SECONDS = 10
DEFAULT_REPORT_DIR = "/data/plugin_reports/stream-stats"
SHARED_EVENTS = ("stream_demoted", "stream_restored")
NO_ACCOUNT = 0
ORDER_KEEP_DAYS = 14
# Dispatcharr's default wait with no data before it gives up on a stream.
STALL_SECONDS = 8


def utc_day(moment):
    return time.strftime("%Y-%m-%d", time.gmtime(moment))


def _as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class Service:
    def __init__(self, store, dispatcharr, clock=time.time):
        self.store = store
        self.dp = dispatcharr
        self.clock = clock

    def settings(self):
        return engine.Settings(self.dp.plugin_state()[1])

    # Events. Each handler reads only the fields it needs, so nothing else can be stored.

    def handle_event(self, event, payload):
        self.store.ensure()
        now = self.clock()
        channel_uuid = str(payload.get("channel_id") or "")
        cfg = self.settings()
        if event == "channel_start":
            self._on_start(channel_uuid, payload, now)
        elif event == "stream_switch":
            self._on_switch(channel_uuid, payload, now, cfg)
        elif event == "channel_stop":
            self._on_stop(channel_uuid, now, cfg)
        elif event == "channel_error":
            self._count(payload.get("stream_id"), "errors", now)
        elif event == "channel_buffering":
            self._count(payload.get("stream_id"), "buffering", now)
        elif event == "channel_reconnect":
            self._on_reconnect(channel_uuid, payload, now)
        if event in ("channel_stop", "m3u_refresh"):
            self.daily_job(now, cfg)

    def _account(self, stream_id):
        account = self.dp.stream_account(stream_id)
        return NO_ACCOUNT if account is None else account

    def _count(self, stream_id, column, now, amount=1):
        stream_id = _as_int(stream_id)
        if stream_id:
            self.store.bump(utc_day(now), stream_id, self._account(stream_id), column, amount)

    def _on_start(self, channel_uuid, payload, now):
        stream_id = _as_int(payload.get("stream_id"))
        channel_id = self.dp.channel_id(channel_uuid)
        if not stream_id or channel_id is None:
            return
        session = self.store.get_session(channel_uuid)
        if session and session["stream_id"] == stream_id and now - session["since"] < DUPLICATE_SECONDS:
            return
        self._count(stream_id, "tuneins", now)
        self.store.add_watched(utc_day(now), channel_id)
        self.store.put_session(channel_uuid, channel_id, stream_id, self._account(stream_id), now, False)

    def _close_session(self, session, now, cfg, stalled=False):
        """Add up the time a stream was on air, and whether it saved the channel."""
        played = self._time_with_data(session, now, stalled)
        day = utc_day(now)
        self.store.bump(day, session["stream_id"], session["account_id"], "seconds", played)
        if session["by_failover"] and played >= cfg.rescue_seconds:
            self.store.bump(day, session["stream_id"], session["account_id"], "rescues")
            self.store.add_rescue(day, session["account_id"], session["channel_id"])

    def _time_with_data(self, session, now, stalled):
        """Time a stream actually delivered. A stream that went silent gets no credit for the silence.

        When a channel has stopped, Dispatcharr may already have cleared what it knew, and
        then the whole time counts. After a stall, Dispatcharr only moved on because nothing had
        arrived for at least this long, and anything newer belongs to the stream it moved to.
        """
        since = session["since"]
        last = self.dp.last_data_at(session["channel_uuid"])
        if stalled and (last is None or last > now - STALL_SECONDS):
            last = now - STALL_SECONDS
        return max(min(now, last if last is not None else now) - since, 0.0)

    def _on_switch(self, channel_uuid, payload, now, cfg):
        channel_id = self.dp.channel_id(channel_uuid)
        if channel_id is None:
            return
        previous = _as_int(payload.get("previous_stream_id"))
        new = _as_int(payload.get("stream_id"))
        reason = payload.get("reason") or "unknown"
        session = self.store.get_session(channel_uuid)
        if session and new and session["stream_id"] == new and now - session["since"] < DUPLICATE_SECONDS:
            return
        column = engine.FAILURE_COLUMN.get(reason)
        if session:
            previous = previous or session["stream_id"]
            self._close_session(session, now, cfg, stalled=reason == "health_monitor")
        if column and previous:
            self._count(previous, column, now)
            self._count(previous, "skipped", now)
        if new:
            self._count(new, "switch_ins", now)
            self.store.put_session(channel_uuid, channel_id, new, self._account(new), now, bool(column))

    def _on_stop(self, channel_uuid, now, cfg):
        session = self.store.get_session(channel_uuid)
        if not session:
            return
        self._close_session(session, now, cfg)
        self.store.delete_session(channel_uuid)

    def _on_reconnect(self, channel_uuid, payload, now):
        # Dispatcharr sends a false reconnect after every manual switch.
        session = self.store.get_session(channel_uuid)
        if session and not session["by_failover"] and now - session["since"] < FALSE_RECONNECT_SECONDS:
            return
        self._count(payload.get("stream_id"), "reconnects", now)

    def on_shared_row(self, event_type, channel_uuid, details):
        """A row written by a demotion plugin, if one is installed. Not needed to work."""
        self.store.ensure()
        details = details if isinstance(details, dict) else {}
        stream_id = _as_int(details.get("stream_id"))
        channel_id = self.dp.channel_id(str(channel_uuid or ""))
        if not stream_id or channel_id is None:
            return
        day = utc_day(self.clock())
        account = self._account(stream_id)
        if event_type == "stream_demoted":
            cause = str(details.get("cause") or "")[:40]
            self.store.add_knockdown(day, channel_id, stream_id, account, "event", cause)
        elif event_type == "stream_restored":
            self.store.add_restore(day, channel_id, stream_id, account)

    # The daily job

    def daily_job(self, now=None, cfg=None):
        now = self.clock() if now is None else now
        cfg = cfg or self.settings()
        day = utc_day(now)
        if self.store.claim_marker("snapshot_day", day):
            self.take_snapshot(day)
            self.store.prune(utc_day(now - cfg.retention_days * 86400), utc_day(now - ORDER_KEEP_DAYS * 86400))

    def take_snapshot(self, day=None):
        """Record today's order and rank counts, and see which streams fell since the last one."""
        self.store.ensure()
        day = day or utc_day(self.clock())
        orders = self.store.read_all_orders()
        previous = self.store.latest_orders_before(day)
        moved = []
        for channel_id, stream_ids in orders.items():
            self.store.save_order(day, channel_id, stream_ids)
            if channel_id in previous:
                old_day, old_ids = previous[channel_id]
                for stream_id, _old, _new in engine.moved_down(old_ids, stream_ids):
                    if not self.store.knockdown_since(channel_id, stream_id, old_day):
                        moved.append((channel_id, stream_id))
        accounts = self.store.stream_accounts(sorted({stream_id for _, stream_id in moved}))
        for channel_id, stream_id in moved:
            self.store.add_knockdown(day, channel_id, stream_id, accounts.get(stream_id, NO_ACCOUNT), "snapshot", None)
        self.store.clear_rank_day(day)
        for row in self.store.ranks_now():
            self.store.save_rank_day(day, row["account_id"], int(row["top1"]), int(row["top3"]), int(row["top5"]))
        return {"channels": len(orders), "knocked_down": len(moved)}

    # The report

    def build_report(self, cfg=None):
        self.store.ensure()
        cfg = cfg or self.settings()
        now = self.clock()
        self.daily_job(now, cfg)
        first_day = utc_day(now - cfg.report_days * 86400)
        channels_total = self.store.channels_total()
        raw = self._gather(first_day, now, cfg)
        totals = {name: sum(item[name] for item in raw) for name in ("top1", "top3", "top5", "seconds")}
        totals["channels_watched"] = self.store.channels_watched(first_day)
        rows = [engine.provider_row(item, cfg, channels_total, totals) for item in raw]
        rows.sort(key=lambda row: (-row["good_streams"], -row["streams_in_channels"], row["provider"]))
        return {
            "generated_utc": time.strftime("%Y-%m-%d %H:%M", time.gmtime(now)),
            "window_days": int(cfg.report_days),
            "channels_total": channels_total,
            "channels_watched": totals["channels_watched"],
            "channels_unobservable": self.dp.redirect_channels(),
            "providers": rows,
        }

    def _gather(self, first_day, now, cfg):
        names = self.dp.providers()
        totals = {row["account_id"]: row for row in self.store.provider_totals(first_day)}
        ranks = {row["account_id"]: row for row in self.store.ranks_now()}
        positions = self.store.positions_now()
        streams, used, only = self.store.reach_now()
        streams = {row["account_id"]: row for row in streams}
        used = {row["account_id"]: row for row in used}
        only = {row["account_id"]: int(row["n"]) for row in only}
        restored = self.store.restored_counts(first_day)
        rescued = self.store.rescued_channels(first_day)
        watched = self.store.watched_by_account(first_day)
        earliest_top1 = self.store.rank_day_at_or_after(first_day)
        # Seven calendar days each, counting today.
        recent = self.store.failure_window(utc_day(now - 6 * 86400), utc_day(now))
        older = self.store.failure_window(utc_day(now - 13 * 86400), utc_day(now - 7 * 86400))
        stream_rows = self.store.stream_totals(first_day)
        knocked = {}
        knocked_by_account = {}
        for row in self.store.knocked_streams(first_day):
            moves = int(row["moves"] or 0)
            knocked[row["stream_id"]] = (moves, int(row["restored"] or 0))
            if moves > 0:
                knocked_by_account[row["account_id"]] = knocked_by_account.get(row["account_id"], 0) + 1
        per_account = {}
        for row in stream_rows:
            tries, failures = float(row["tries"] or 0), float(row["failures"] or 0)
            moves, back = knocked.get(row["stream_id"], (0, 0))
            bucket = per_account.setdefault(row["account_id"], {"checked": 0, "failed": 0, "good": 0, "checked_knocked": 0})
            if tries > 0:
                bucket["checked"] += 1
                if engine.is_good(tries, failures, moves, back):
                    bucket["good"] += 1
                if moves > 0:
                    bucket["checked_knocked"] += 1
            if failures > 0:
                bucket["failed"] += 1
        resolutions = self.dp.resolutions()

        account_ids = set(names) | set(totals) | set(streams)
        account_ids.discard(NO_ACCOUNT)
        out = []
        for account_id in account_ids:
            info = names.get(account_id, {"name": f"Removed account {account_id}", "limit": None})
            total = totals.get(account_id, {})
            rank = ranks.get(account_id, {})
            reach = streams.get(account_id, {})
            usage = used.get(account_id, {})
            bucket = per_account.get(account_id, {"checked": 0, "failed": 0, "good": 0, "checked_knocked": 0})
            place = positions.get(account_id)
            now_rate, old_rate = recent.get(account_id, (0, 0)), older.get(account_id, (0, 0))
            first_top1 = earliest_top1.get(account_id)
            out.append({
                "account_id": account_id,
                "provider": info["name"],
                "connection_limit": info["limit"],
                "streams_total": int(reach.get("total") or 0),
                "streams_stale": int(reach.get("stale") or 0),
                "streams_in_channels": int(usage.get("in_channels") or 0),
                "channels_served": int(usage.get("channels") or 0),
                "channels_only_here": only.get(account_id, 0),
                "watched_channels_served": watched.get(account_id, 0),
                "top1": int(rank.get("top1") or 0),
                "top3": int(rank.get("top3") or 0),
                "top5": int(rank.get("top5") or 0),
                "median_position": statistics.median(place) if place else None,
                "top1_change": int(rank.get("top1") or 0) - first_top1 if first_top1 is not None else None,
                "tuneins": int(total.get("tuneins") or 0),
                "switch_ins": int(total.get("switch_ins") or 0),
                "fail_hard": int(total.get("fail_hard") or 0),
                "fail_stall": int(total.get("fail_stall") or 0),
                "fail_slow": int(total.get("fail_slow") or 0),
                "failure_change_7d": engine.rate_change(now_rate[0], now_rate[1], old_rate[0], old_rate[1]),
                "skipped": int(total.get("skipped") or 0),
                "rescues": int(total.get("rescues") or 0),
                "channels_rescued": rescued.get(account_id, 0),
                "seconds": float(total.get("seconds") or 0),
                "streams_checked": bucket["checked"],
                "streams_failed": bucket["failed"],
                "good_streams": bucket["good"],
                "streams_knocked_down": knocked_by_account.get(account_id, 0),
                "checked_knocked_down": bucket["checked_knocked"],
                "streams_restored": restored.get(account_id, 0),
                "resolutions": resolutions.get(account_id, ""),
            })
        return out

    def summary(self, cfg=None):
        report = self.build_report(cfg)
        if not report["providers"]:
            return report, "No providers found yet."
        lines = [engine.summary_line(row) for row in report["providers"]]
        coverage = f"{report['channels_watched']} of {report['channels_total']} channels watched in {report['window_days']} days."
        return report, coverage + " " + " | ".join(lines)

    def write_report(self, folder, cfg=None):
        report = self.build_report(cfg)
        os.makedirs(folder, exist_ok=True)
        html_path = os.path.join(folder, "provider-report.html")
        with open(html_path, "w", encoding="utf-8") as handle:
            handle.write(render_html(report))
        with open(os.path.join(folder, "provider-report.csv"), "w", encoding="utf-8", newline="") as handle:
            handle.write(render_csv(report))
        with open(os.path.join(folder, "provider-report.json"), "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=1)
        return html_path, report


def _safe_cell(value):
    # A provider name starting with one of these would run as a formula in a spreadsheet.
    if isinstance(value, str) and value[:1] in ("=", "+", "-", "@"):
        return "'" + value
    return "" if value is None else value


def render_csv(report):
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow([label for _key, label in engine.REPORT_COLUMNS])
    for row in report["providers"]:
        writer.writerow([_safe_cell(row.get(key)) for key, _label in engine.REPORT_COLUMNS])
    return buffer.getvalue()


NOTES = (
    "Tries are tune-ins plus failovers onto a stream. Failures per 100 tries lets a big provider and a small one be compared.",
    "Streams checked are the ones viewers actually tried. Streams nobody watched have no score, and this plugin won't open them to find out.",
    "A good stream was tried, worked more often than it failed, and wasn't left knocked down. The estimate applies that rate to all the provider's streams in channels, so treat it as a guess. It's left blank with too few streams tried.",
    "Ranked first, top 3 and top 5 count a provider's streams in those places across all channels. The percentages are its share of those places, so each column adds up to 100.",
    "Only this provider counts channels you would lose by dropping it. Rescues are failovers onto a stream that then held for at least 30 seconds.",
    "Data says how far to trust a row, from how many tries and how many different streams it rests on: unproven under 20 tries or 5 streams, some under 100 tries or 20 streams, solid above that (all settings).",
)


def render_html(report):
    head = "".join(f"<th>{html.escape(label)}</th>" for _key, label in engine.REPORT_COLUMNS)
    body = []
    for row in report["providers"]:
        cells = []
        for key, _label in engine.REPORT_COLUMNS:
            value = row.get(key)
            text = "" if value is None else str(value)
            cells.append(f"<td>{html.escape(text)}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    notes = "".join(f"<li>{html.escape(note)}</li>" for note in NOTES)
    coverage = (
        f"{report['channels_watched']} of {report['channels_total']} channels were watched in the last "
        f"{report['window_days']} days. {report['channels_unobservable']} channels use a redirect profile, so they can't be observed."
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Provider report</title>
<style>
:root {{ color-scheme: light dark; --line: #8884; }}
body {{ font: 14px/1.45 system-ui, sans-serif; margin: 16px; }}
.wrap {{ overflow-x: auto; }}
table {{ border-collapse: collapse; white-space: nowrap; }}
th, td {{ border: 1px solid var(--line); padding: 4px 8px; text-align: right; }}
th {{ white-space: normal; min-width: 64px; font-weight: 600; }}
th:first-child, td:first-child {{ text-align: left; position: sticky; left: 0; background: Canvas; }}
</style></head><body>
<h1>Provider report</h1>
<p>Built {html.escape(report['generated_utc'])} UTC. {html.escape(coverage)}</p>
<div class="wrap"><table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table></div>
<h2>How to read it</h2><ul>{notes}</ul>
</body></html>
"""
