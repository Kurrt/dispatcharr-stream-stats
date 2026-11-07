"""Stream Stats: per-provider numbers from what viewers actually did.

Read only: it changes nothing in Dispatcharr and writes only its own tables.
"""

from __future__ import annotations

import logging
import os
import sys

if __package__:
    from .stream_stats import service as service_module
    from .stream_stats.dispatcharr import Dispatcharr
    from .stream_stats.service import SHARED_EVENTS, Service
    from .stream_stats.store import Store
else:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from stream_stats import service as service_module
    from stream_stats.dispatcharr import Dispatcharr
    from stream_stats.service import SHARED_EVENTS, Service
    from stream_stats.store import Store

logger = logging.getLogger(__name__)

# Dispatcharr names a plugin after its folder.
PLUGIN_KEY = os.path.basename(os.path.dirname(os.path.abspath(__file__))).replace(" ", "_").lower()
DISPATCH_UID = "stream_stats_shared_rows"

_service = None


def get_service():
    global _service
    if _service is None:
        from django.db import connection

        _service = Service(Store(lambda: connection.cursor()), Dispatcharr(PLUGIN_KEY))
    return _service


def _on_system_event(sender, instance, created=False, **_kwargs):
    # This runs inside whatever wrote the row, so anything else leaves at once.
    if not created or instance.event_type not in SHARED_EVENTS:
        return
    try:
        service = get_service()
        if service.dp.plugin_state()[0]:
            service.on_shared_row(instance.event_type, instance.channel_id, instance.details)
    except Exception:
        logger.exception("Could not read a shared row")


def _listen_for_shared_rows():
    try:
        from core.models import SystemEvent
        from django.db.models.signals import post_save

        post_save.connect(_on_system_event, sender=SystemEvent, dispatch_uid=DISPATCH_UID, weak=False)
    except Exception:
        logger.exception("Could not listen for shared rows")


if __package__:
    _listen_for_shared_rows()


class Plugin:
    name = "Stream Stats"
    version = "0.2.0"
    description = "Per-provider numbers from what viewers actually did, to help decide which provider to keep. Read only."

    def run(self, action, params, context):
        params = params or {}
        context = context or {}
        service = get_service()
        if action == "on_event":
            # The UI also shows a Run button for this action. With no event it does nothing.
            event = params.get("event")
            if event:
                service.handle_event(event, params.get("payload") or {})
            return {"status": "ok"}
        cfg = service_module.engine.Settings(context.get("settings"))
        if action == "summary":
            report, message = service.summary(cfg)
            return {"status": "ok", "message": message, "report": report}
        if action == "report":
            # Not context["data_dir"]: a later Dispatcharr fills that in, and the folder would move.
            folder = cfg.report_folder.strip() or service_module.DEFAULT_REPORT_DIR
            path, report = service.write_report(folder, cfg)
            return {
                "status": "ok",
                "message": f"Report written next to {path}, with CSV and JSON copies.",
                "file": path,
                "report": report,
            }
        if action == "snapshot":
            result = service.take_snapshot()
            return {
                "status": "ok",
                "message": f"Saved the order of {result['channels']} channels. {result['knocked_down']} streams fell since the last one.",
                **result,
            }
        if action == "erase_data":
            service.store.ensure()
            service.store.erase()
            return {"status": "ok", "message": "Erased all collected numbers."}
        return {"status": "error", "message": f"Unknown action {action}"}
