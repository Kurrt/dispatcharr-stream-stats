"""The buttons, through the same entry point Dispatcharr calls."""

import importlib.util
import json
import os
import tempfile
import unittest

from .harness import ROOT, World


def load_plugin_module():
    spec = importlib.util.spec_from_file_location("plugin_under_test", os.path.join(ROOT, "plugin.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Buttons(unittest.TestCase):
    def setUp(self):
        self.module = load_plugin_module()
        self.w = World()
        self.w.provider(1, "A")
        self.w.stream(11, 1)
        self.w.channel(1, [11])
        self.module._service = self.w.service
        self.plugin = self.module.Plugin()
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)

    def run_action(self, action, params=None, **settings):
        settings.setdefault("report_folder", self.folder.name)
        return self.plugin.run(action, params or {}, {"settings": settings})

    def test_event_action_with_no_event_does_nothing(self):
        self.assertEqual(self.run_action("on_event"), {"status": "ok"})

    def test_event_action_passes_events_through(self):
        self.run_action("on_event", {"event": "channel_start", "payload": {"channel_id": self.w.uuid(1), "stream_id": 11}})
        self.assertEqual(self.w.report_row(1)["tries"], 1)

    def test_summary(self):
        result = self.run_action("summary")
        self.assertIn("A:", result["message"])
        self.assertEqual(result["report"]["providers"][0]["provider"], "A")

    def test_report_writes_files(self):
        result = self.run_action("report")
        self.assertTrue(os.path.exists(result["file"]))

    def test_snapshot(self):
        self.assertEqual(self.run_action("snapshot")["channels"], 1)

    def test_erase(self):
        self.run_action("on_event", {"event": "channel_start", "payload": {"channel_id": self.w.uuid(1), "stream_id": 11}})
        self.run_action("erase_data")
        self.assertEqual(self.w.report_row(1)["tries"], 0)

    def test_unknown_action(self):
        self.assertEqual(self.run_action("nope")["status"], "error")

    def test_every_button_in_the_manifest_is_handled(self):
        with open(os.path.join(ROOT, "plugin.json"), encoding="utf-8") as handle:
            actions = [item["id"] for item in json.load(handle)["actions"]]
        for action in actions:
            self.assertNotIn("Unknown action", str(self.run_action(action).get("message")), action)

    def test_shared_row_listener_ignores_other_events(self):
        class Row:
            event_type = "channel_start"
            channel_id = self.w.uuid(1)
            details = {"stream_id": 11}

        self.module._on_system_event(None, Row(), created=True)
        self.assertEqual(self.w.report_row(1)["streams_knocked_down"], 0)

    def test_shared_row_listener_counts_a_demotion(self):
        class Row:
            event_type = "stream_demoted"
            channel_id = self.w.uuid(1)
            details = {"stream_id": 11, "cause": "flag"}

        self.module._on_system_event(None, Row(), created=True)
        self.assertEqual(self.w.report_row(1)["streams_knocked_down"], 1)

    def test_shared_row_listener_does_nothing_when_disabled(self):
        class Row:
            event_type = "stream_demoted"
            channel_id = self.w.uuid(1)
            details = {"stream_id": 11}

        self.w.dp.enabled = False
        self.module._on_system_event(None, Row(), created=True)
        self.assertEqual(self.w.report_row(1)["streams_knocked_down"], 0)

    def test_shared_row_listener_never_raises(self):
        class Row:
            event_type = "stream_demoted"
            channel_id = "bad"
            details = "not a dict"

        self.module._on_system_event(None, Row(), created=True)
