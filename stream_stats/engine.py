"""The sums. No Django, no database and no network, so each one can be tested alone."""

from __future__ import annotations

DEFAULTS = {
    "report_days": 30,
    "retention_days": 90,
    "rescue_seconds": 30,
    "thin_tries": 20,
    "solid_tries": 100,
    "thin_streams": 5,
    "solid_streams": 20,
    "report_folder": "",
}

FAILURE_COLUMN = {
    "max_retries_exceeded": "fail_hard",
    "health_monitor": "fail_stall",
    "buffering_timeout": "fail_slow",
}


class Settings:
    def __init__(self, raw=None):
        raw = raw if isinstance(raw, dict) else {}
        for name, default in DEFAULTS.items():
            value = raw.get(name, default)
            if isinstance(default, str):
                setattr(self, name, "" if value is None else str(value))
                continue
            try:
                number = float(value)
            except (TypeError, ValueError):
                number = default
            setattr(self, name, default if number != number or number < 0 else number)


def moved_down(before, after):
    """Which streams were really moved down between two orders.

    When one stream jumps to the top, the others shift down a place without anyone moving
    them. So the streams that kept their order against each other (the longest increasing
    run) count as still, and only the rest count as moved.

    Returns [(stream_id, old position, new position)], positions counted among the streams
    in both lists.
    """
    shared = set(before) & set(after)
    old = [item for item in before if item in shared]
    new = [item for item in after if item in shared]
    rank = {item: index for index, item in enumerate(old)}
    still = _longest_increasing_run([rank[item] for item in new])
    result = []
    for position, item in enumerate(new):
        if rank[item] not in still and position > rank[item]:
            result.append((item, rank[item], position))
    return result


def _longest_increasing_run(values):
    """The longest increasing run, preferring the one that starts earliest in the new order.

    If [1, 2, 3, 4] becomes [3, 4, 1, 2], either pair could count as the still one. Taking
    3 and 4 reads it as 1 and 2 being dropped, which is how a demotion looks.
    """
    count = len(values)
    best = [1] * count
    for index in range(count - 1, -1, -1):
        for later in range(index + 1, count):
            if values[later] > values[index]:
                best[index] = max(best[index], best[later] + 1)
    kept = set()
    remaining = max(best, default=0)
    floor = -1
    for index in range(count):
        if remaining and best[index] == remaining and values[index] > floor:
            kept.add(values[index])
            floor = values[index]
            remaining -= 1
    return kept


def percent(part, whole):
    return round(100.0 * part / whole, 1) if whole else None


def share(part, whole):
    """A percentage of something the part belongs to, so it can't pass 100.

    It could otherwise: a stream that was checked and has since left its channel still
    counts as checked, but no longer counts in the whole.
    """
    return percent(min(part, whole), whole) if whole else None


def rate_change(recent_failures, recent_tries, older_failures, older_tries):
    """Failures per 100 tries, this period against the one before. None if either is empty."""
    if not recent_tries or not older_tries:
        return None
    return round(100.0 * recent_failures / recent_tries - 100.0 * older_failures / older_tries, 1)


def confidence(tries, checked, in_channels, cfg):
    """How far to trust a row. A hundred tries of one stream say little about the other thousand."""
    thin = min(cfg.thin_streams, in_channels) if in_channels else cfg.thin_streams
    solid = min(cfg.solid_streams, in_channels) if in_channels else cfg.solid_streams
    if tries < cfg.thin_tries or checked < thin:
        return "unproven"
    if tries < cfg.solid_tries or checked < solid:
        return "some"
    return "solid"


def is_good(tries, failures, moves, restored):
    """A good stream was tried, worked more often than it failed, and wasn't left knocked down."""
    return tries > 0 and failures * 2 < tries and moves <= restored


def provider_row(raw, cfg, channels_total, totals):
    """Turn raw counts for one provider into the row a report shows."""
    tries = raw["tuneins"] + raw["switch_ins"]
    failures = raw["fail_hard"] + raw["fail_stall"] + raw["fail_slow"]
    checked = raw["streams_checked"]
    failed = raw["streams_failed"]
    knocked = raw["streams_knocked_down"]
    good = raw["good_streams"]
    in_channels = raw["streams_in_channels"]
    good_rate = good / checked if checked else None
    # Too few streams tried, and any figure for the rest would be made up.
    enough_to_estimate = good_rate is not None and checked >= min(cfg.thin_streams, in_channels or cfg.thin_streams)
    return {
        "account_id": raw["account_id"],
        "provider": raw["provider"],
        "connection_limit": raw["connection_limit"],
        "streams_total": raw["streams_total"],
        "streams_gone_stale": raw["streams_stale"],
        "streams_in_channels": in_channels,
        "channels_served": raw["channels_served"],
        "channels_share_pct": percent(raw["channels_served"], channels_total),
        "channels_only_here": raw["channels_only_here"],
        "watched_channels_served": raw["watched_channels_served"],
        "watched_channels_share_pct": percent(raw["watched_channels_served"], totals["channels_watched"]),
        "ranked_first": raw["top1"],
        "ranked_first_pct": percent(raw["top1"], totals["top1"]),
        "ranked_top3": raw["top3"],
        "ranked_top3_pct": percent(raw["top3"], totals["top3"]),
        "ranked_top5": raw["top5"],
        "ranked_top5_pct": percent(raw["top5"], totals["top5"]),
        "median_position": raw["median_position"],
        "ranked_first_change": raw["top1_change"],
        "tries": tries,
        "streams_checked": checked,
        "streams_checked_pct": share(checked, in_channels),
        "failures": failures,
        "failures_per_100_tries": percent(failures, tries),
        "fail_hard": raw["fail_hard"],
        "fail_stall": raw["fail_stall"],
        "fail_slow": raw["fail_slow"],
        "failure_change_7d": raw["failure_change_7d"],
        "streams_failed": failed,
        "streams_failed_pct": share(failed, checked),
        "skipped": raw["skipped"],
        "skipped_pct": share(raw["skipped"], tries),
        "streams_knocked_down": knocked,
        "streams_knocked_down_pct": share(raw["checked_knocked_down"], checked),
        "streams_restored": raw["streams_restored"],
        "good_streams": good,
        "good_streams_pct": share(good, checked),
        "good_streams_estimate": round(good_rate * in_channels) if enough_to_estimate else None,
        "rescues": raw["rescues"],
        "channels_rescued": raw["channels_rescued"],
        "hours_on_air": round(raw["seconds"] / 3600.0, 2),
        "hours_share_pct": percent(raw["seconds"], totals["seconds"]),
        "hours_per_failure": round(raw["seconds"] / 3600.0 / failures, 2) if failures else None,
        "resolutions": raw.get("resolutions", ""),
        "confidence": confidence(tries, checked, in_channels, cfg),
    }


REPORT_COLUMNS = [
    ("provider", "Provider"),
    ("confidence", "Data"),
    ("connection_limit", "Connection limit"),
    ("streams_total", "Streams"),
    ("streams_gone_stale", "Gone stale"),
    ("streams_in_channels", "In channels"),
    ("channels_served", "Channels served"),
    ("channels_share_pct", "Share of channels %"),
    ("channels_only_here", "Only this provider"),
    ("watched_channels_served", "Watched channels served"),
    ("watched_channels_share_pct", "Share of watched channels %"),
    ("ranked_first", "Ranked first"),
    ("ranked_first_pct", "Ranked first %"),
    ("ranked_top3", "Top 3"),
    ("ranked_top3_pct", "Top 3 %"),
    ("ranked_top5", "Top 5"),
    ("ranked_top5_pct", "Top 5 %"),
    ("median_position", "Median place"),
    ("ranked_first_change", "Ranked first change"),
    ("tries", "Tries"),
    ("streams_checked", "Streams checked"),
    ("streams_checked_pct", "Checked %"),
    ("failures", "Failures"),
    ("failures_per_100_tries", "Failures per 100 tries"),
    ("fail_hard", "Hard"),
    ("fail_stall", "Stall"),
    ("fail_slow", "Slow"),
    ("failure_change_7d", "Change, last 7 days"),
    ("streams_failed", "Streams that failed"),
    ("streams_failed_pct", "Failed %"),
    ("skipped", "Skipped"),
    ("skipped_pct", "Skipped % of tries"),
    ("streams_knocked_down", "Knocked down"),
    ("streams_knocked_down_pct", "Knocked down %"),
    ("streams_restored", "Restored"),
    ("good_streams", "Good streams"),
    ("good_streams_pct", "Good %"),
    ("good_streams_estimate", "Good, estimated across all in channels"),
    ("rescues", "Rescues"),
    ("channels_rescued", "Channels rescued"),
    ("hours_on_air", "Hours on air"),
    ("hours_share_pct", "Share of hours on air %"),
    ("hours_per_failure", "Hours per failure"),
    ("resolutions", "Resolutions seen"),
]


def summary_line(row):
    rate = row["failures_per_100_tries"]
    rate_text = "no tries yet" if rate is None else f"{rate:g} failures per 100 tries"
    return (
        f"{row['provider']}: {row['good_streams']} good of {row['streams_checked']} checked, "
        f"{rate_text}, first on {row['ranked_first']} channels, "
        f"{row['channels_only_here']} only here ({row['confidence']})"
    )
