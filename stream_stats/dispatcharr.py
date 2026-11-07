"""The only module that touches Dispatcharr's own code. Every call here is a read."""

from __future__ import annotations

import logging
import uuid as uuid_module

logger = logging.getLogger(__name__)


class Dispatcharr:
    def __init__(self, plugin_key):
        self.plugin_key = plugin_key

    def plugin_state(self):
        from apps.plugins.models import PluginConfig

        row = PluginConfig.objects.filter(key=self.plugin_key).values("enabled", "settings").first()
        if not row:
            return False, {}
        return bool(row["enabled"]), row["settings"] or {}

    def channel_id(self, channel_uuid):
        from apps.channels.models import Channel

        try:
            value = uuid_module.UUID(str(channel_uuid))
        except ValueError:
            return None
        return Channel.objects.filter(uuid=value).values_list("id", flat=True).first()

    def last_data_at(self, channel_uuid):
        """When Dispatcharr last received data for a channel, if it still knows. Read from Redis."""
        from core.utils import RedisClient

        try:
            client = RedisClient.get_client()
            value = client.get(f"live:channel:{channel_uuid}:last_data") if client else None
            return float(value) if value else None
        except Exception:
            logger.debug("Could not read when data last arrived", exc_info=True)
            return None

    def stream_account(self, stream_id):
        from apps.channels.models import Stream

        return Stream.objects.filter(pk=stream_id).values_list("m3u_account_id", flat=True).first()

    def providers(self):
        """Names and connection limits per account, read fresh for each report."""
        from apps.m3u.models import M3UAccount, M3UAccountProfile

        limits = {}
        for account_id, limit in M3UAccountProfile.objects.filter(is_default=True).values_list("m3u_account_id", "max_streams"):
            limits[account_id] = limit
        return {
            account_id: {"name": name, "limit": limits.get(account_id)}
            for account_id, name in M3UAccount.objects.values_list("id", "name")
        }

    def redirect_channels(self):
        """Channels on a Redirect profile send viewers straight to the provider, so no events."""
        from apps.channels.models import Channel
        from core.models import REDIRECT_PROFILE_NAME, CoreSettings, StreamProfile

        ids = list(StreamProfile.objects.filter(name=REDIRECT_PROFILE_NAME, locked=True).values_list("id", flat=True))
        if not ids:
            return 0
        channels = Channel.objects.with_effective_values()
        count = channels.filter(effective_stream_profile_id__in=ids).count()
        # A channel with no profile of its own uses the default one.
        if CoreSettings.get_default_stream_profile_id() in ids:
            count += channels.filter(effective_stream_profile_id__isnull=True).count()
        return count

    def resolutions(self):
        """Resolution mix per account, from what FFmpeg profiles have already recorded."""
        from apps.channels.models import Stream

        counts = {}
        rows = Stream.objects.filter(m3u_account__isnull=False, stream_stats__isnull=False).values_list(
            "m3u_account_id", "stream_stats"
        )
        for account_id, stats in rows:
            value = (stats or {}).get("resolution") if isinstance(stats, dict) else None
            if value:
                bucket = counts.setdefault(account_id, {})
                bucket[str(value)] = bucket.get(str(value), 0) + 1
        return {
            account_id: ", ".join(f"{name} x{count}" for name, count in sorted(bucket.items(), key=lambda item: -item[1])[:4])
            for account_id, bucket in counts.items()
        }
