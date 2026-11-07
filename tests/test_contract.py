"""Promises that must hold however the code changes."""

import ast
import json
import os
import re
import unittest

from .harness import ROOT, engine

SHIPPED = ["plugin.py"] + [
    os.path.join("stream_stats", name)
    for name in sorted(os.listdir(os.path.join(ROOT, "stream_stats")))
    if name.endswith(".py")
]

BANNED_MODULES = {"requests", "urllib", "urllib2", "urllib3", "http", "httpx", "aiohttp", "socket", "subprocess", "ftplib", "pycurl"}
BANNED_NAMES = {"get_stream", "validate_stream_url", "ffprobe", "ffmpeg", "urlopen", "Popen", "system", "change_stream_url", "stream_demote"}
# A write to anything but its own tables would need one of these.
BANNED_WRITES = {"bulk_update", "bulk_create", "update_or_create", "get_or_create", "select_for_update"}

EVENTS_DISPATCHARR_DELIVERS = {
    "channel_start", "channel_stop", "channel_reconnect", "channel_error", "channel_buffering", "channel_failover",
    "stream_switch", "client_connect", "client_disconnect", "recording_start", "recording_end", "epg_refresh",
    "epg_error", "m3u_refresh", "m3u_error", "login_failed", "epg_blocked", "m3u_blocked", "vod_start", "vod_stop",
}


def manifest():
    with open(os.path.join(ROOT, "plugin.json"), encoding="utf-8") as handle:
        return json.load(handle)


def source(path):
    with open(os.path.join(ROOT, path), encoding="utf-8") as handle:
        return handle.read()


class ReadOnlyAndPassive(unittest.TestCase):
    def test_no_network_probe_or_write_code(self):
        for path in SHIPPED:
            for node in ast.walk(ast.parse(source(path), filename=path)):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        self.assertNotIn(alias.name.split(".")[0], BANNED_MODULES, path)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    self.assertNotIn(node.module.split(".")[0], BANNED_MODULES, path)
                elif isinstance(node, ast.Attribute):
                    self.assertNotIn(node.attr, BANNED_NAMES | BANNED_WRITES, f"{path}:{node.lineno}")
                elif isinstance(node, ast.Name):
                    self.assertNotIn(node.id, BANNED_NAMES, f"{path}:{node.lineno}")

    def test_sql_only_writes_its_own_tables(self):
        text = source(os.path.join("stream_stats", "store.py"))
        for verb in ("INSERT INTO", "UPDATE", "DELETE FROM"):
            for match in re.finditer(r"(?<!DO )" + verb + r"\s+(\S+)", text):
                target = match.group(1)
                self.assertTrue(target.startswith("{PREFIX}"), f"{verb} {target}")

    def test_no_import_of_any_other_plugin(self):
        for path in SHIPPED:
            self.assertNotIn("demote", re.sub(r'"stream_demoted"|stream_demoted', "", source(path)).lower(), path)

    def test_viewer_events_are_not_subscribed(self):
        events = next(a["events"] for a in manifest()["actions"] if a["id"] == "on_event")
        self.assertNotIn("client_connect", events)
        self.assertNotIn("client_disconnect", events)


class Manifest(unittest.TestCase):
    def test_required_store_fields(self):
        data = manifest()
        for name in ("name", "version", "description", "author", "license"):
            self.assertTrue(data.get(name), name)
        self.assertEqual(data["license"], "MIT")
        self.assertRegex(data["version"], r"^\d+\.\d+\.\d+$")

    def test_field_defaults_match_the_code(self):
        fields = {f["id"]: f for f in manifest()["fields"]}
        self.assertEqual(set(fields), set(engine.DEFAULTS))
        for name, default in engine.DEFAULTS.items():
            self.assertEqual(fields[name]["default"], default, name)

    def test_only_real_events_are_subscribed(self):
        for action in manifest()["actions"]:
            for event in action.get("events", []):
                self.assertIn(event, EVENTS_DISPATCHARR_DELIVERS)
