# Stream Stats

A Dispatcharr plugin that shows, per provider, what viewers actually got, to help you decide
which providers are worth keeping.

A provider with ten times the streams and more broken ones can still be worth more, because it
brings more good ones. So this reports both: the good streams a provider brings in total, and how
often it fails.

**It's read only.** It changes nothing in Dispatcharr and writes only its own tables. It never
opens a stream, so it uses none of your provider's connections.

Needs Dispatcharr 0.33.0. It has been tested on 0.33.0 only, with a fake provider. See
[What has been tested](#what-has-been-tested).

## What you get

One row per provider, as a one-line summary, an HTML page, a CSV and a JSON file.

**How much it brings**

| Column | Meaning |
|---|---|
| Streams, Gone stale | Streams from this provider, and how many are no longer in its playlist |
| In channels | How many of them are used in a channel |
| Channels served, Share of channels % | Channels with at least one of its streams |
| Only this provider | Channels with no other provider: what you'd lose by dropping it |
| Watched channels served, Share of watched channels % | The same, counting only channels people actually watched |
| Good streams, Good % | Streams viewers tried that worked more often than they failed and weren't left knocked down |
| Good, estimated | That rate applied to all its streams in channels. It's a guess, marked as one, and left blank with under 5 streams tried |

**Where it ranks**

| Column | Meaning |
|---|---|
| Ranked first, Top 3, Top 5 | Its streams in those places, across all channels, and its share of those places |
| Median place | The middle position its streams hold |
| Ranked first change | How much that count has moved over the report window |

**How it behaves**

| Column | Meaning |
|---|---|
| Tries | Tune-ins, plus failovers onto its streams |
| Streams checked, Checked % | Streams viewers actually tried |
| Failures per 100 tries | So a big and a small provider can be compared |
| Hard, Stall, Slow | Failures by the reason Dispatcharr gave |
| Change, last 7 days | Failures per 100 tries against the 7 days before. Calendar days in UTC, counting today |
| Streams that failed, Failed % | How many of the streams checked failed at least once |
| Skipped, Skipped % of tries | Times Dispatcharr moved on from one of its streams because it failed |
| Knocked down, Knocked down % | Streams moved down a channel's order, by anyone, and the share of streams checked that were |
| Restored | Streams put back, if a demotion plugin reports it |
| Rescues, Channels rescued | Failovers onto its streams that then held for 30 seconds |
| Hours on air, Share of hours on air %, Hours per failure | How much it's actually used. Time with no data arriving doesn't count, where Dispatcharr can say when it last arrived |
| Resolutions seen | From what FFmpeg profiles have recorded. Empty on proxy profiles |
| Data | How far to trust the row. `unproven` under 20 tries or 5 different streams, `some` under 100 tries or 20 streams, `solid` above. A small provider needs only as many streams as it has |

The report also says how many channels were watched in the window, and how many use a Redirect
profile and so can't be observed.

## Install

1. In Dispatcharr, open Plugins, Import Plugin, and choose the release zip. Or copy this folder
   to `/data/plugins/stream_stats`, and press the refresh button on the Plugins page.
2. Enable it, then restart the Dispatcharr container so that every worker has it.
3. Press **Save today's order now**, so tomorrow has something to compare with.

Numbers then build up as people watch. Nothing can be filled in from before you installed it.

## Using it

| Button | What it does |
|---|---|
| Summary | One line per provider |
| Write report | Writes `provider-report.html`, `.csv` and `.json` to `/data/plugin_reports/stream-stats` |
| Save today's order now | Happens by itself once a day |
| Erase collected numbers | Starts again. Nothing in Dispatcharr itself changes |

Dispatcharr 0.33.0 gives plugins no page of their own, so the report is a file. It's one
self-contained page with nothing to download, so you can copy it off the server and open it
anywhere. An admin can also fetch the numbers as JSON from Dispatcharr's API by running the
`report` action.

## Settings

| Setting | Default | Meaning |
|---|---|---|
| Report window | 30 days | How far back reports look |
| Keep data | 90 days | Older numbers are deleted |
| Rescue counts after | 30 seconds | A failover must hold this long to count |
| Unproven below | 20 tries | |
| Solid above | 100 tries | |
| Data: unproven below | 5 streams | So many tries of a few streams don't look like proof about a whole provider |
| Data: solid above | 20 streams | |
| Report folder | empty | Empty means `/data/plugin_reports/stream-stats` |

## Where the numbers come from

It needs nothing else installed.

- **Events** Dispatcharr already sends: `channel_start`, `stream_switch`, `channel_error`,
  `channel_reconnect`, `channel_buffering`, `channel_stop` and `m3u_refresh`. It doesn't subscribe
  to the client events, so details about viewers never reach it.
- **Rankings** from Dispatcharr's stream order, read when a report is built.
- **Knock-downs** by comparing today's order with the last one it saved. When one stream drops, the
  ones below it rise, and when one jumps up, others shift down without being moved. It works out
  which streams were really moved, so a stream that only shifted doesn't count.

**With a demotion plugin.** If a plugin writes `stream_demoted` or `stream_restored` rows to
Dispatcharr's System Events, this one counts them straight away, and records the cause. A move it
hears about that way isn't counted again by the comparison. Neither plugin imports the other. With
no such plugin, it carries on with the daily comparison. Other tools that sort streams, such as
Stream-Mapparr, also show up as knock-downs in that comparison, because it can't tell who moved a stream.

## What it can't tell you

- **Streams nobody watches have no score.** Finding out would mean opening them, which uses a
  provider connection. Trust a row by its `Data` column.
- **A bad picture.** Dispatcharr can't detect a stream sending rubbish or a black screen in proxy
  mode, so it isn't a failure here. If a demotion plugin that checks content moved the stream, its
  cause is kept with the knock-down.
- **Skips because a provider was full.** Dispatcharr moves past a full account with no event, so
  those can't be counted.
- **Redirect profiles, and clients using provider URLs directly.** Dispatcharr doesn't see them.
  The report counts Redirect channels so you can see the gap.
- **Tune-in time.** It's left out on purpose. The only passive measure includes filling a buffer,
  so it depends on the stream's bitrate, and can't fairly compare providers.
- **Knock-downs inside one day**, with no demotion plugin. A stream moved down and back between
  two daily comparisons isn't seen.
- **Resolution** on proxy profiles, because only FFmpeg reports it. Bitrate and codec are left out
  for the same reason.
- **Cost, and your provider's real connection count.** Dispatcharr doesn't know them.
- **Anything before you installed it.** Dispatcharr keeps only a short rolling log.

Figures are per calendar day in UTC. Time on air counts on the day a session ends. A viewer leaving a
stream that had gone silent can still add that time, because Dispatcharr may have cleared its record by then.

## What it keeps for later

Each stream's tries, failures by reason, reconnects, buffering events, skips, rescues and time on air, per day, for 90 days.
That's enough to rank working streams by how often they fail per hour of play, should that be wanted one day. It isn't
per channel, and it has no time to first data, so it can't rank by how fast a stream starts.

## What's stored

Numbers, and short codes for why a stream was moved down: stream ids, channel ids, account ids, counts
and times, in tables named `plugin_stream_stats_*`. Those tables are included in Dispatcharr's backups.

Stream URLs, credentials, stream names, channel names, IP addresses, user names and user agents are
never stored or exported. Event payloads do hold URLs with provider logins in them, so each handler
picks out only the fields it needs. Provider names are read fresh when a report is built, and are in
the report, so check them before you share one.

## Undo and removal

There's nothing to undo in Dispatcharr, because this plugin never changes it. To start the numbers
again, press **Erase collected numbers**.

To remove it, press **Erase collected numbers**, remove the plugin, and if you want the empty tables
gone, run:

```sql
DROP TABLE plugin_stream_stats_stream_day, plugin_stream_stats_session, plugin_stream_stats_rank,
  plugin_stream_stats_rank_day, plugin_stream_stats_knockdown, plugin_stream_stats_rescue,
  plugin_stream_stats_watched, plugin_stream_stats_marker;
```

## Limits

- **Dispatcharr 0.34 will change where event handlers run.** A repeated event counts once and handlers
  don't rely on running in the proxy process, but 0.34 hasn't been tested. Events arriving out of order
  could put a few seconds of time on air against the wrong stream.
- **A restart while a channel is playing** can lose that session's time on air, because no stop event
  comes. Failure counts aren't affected.
- **The SQL runs on PostgreSQL**, which is what was tested, and on SQLite in the unit tests. A Dispatcharr
  running on SQLite has not been tried.

## What has been tested

On a throwaway Dispatcharr 0.33.0 on an isolated network, with a fake provider that counts every
connection, and test viewers that read the stream but don't play it. Each container had one CPU.
Most of this was run on this version and some on the one before it, and nothing that affects those results
changed between them.

- **No extra connections.** The same viewers ran with no plugin and with this one and a demotion plugin in dry run and live. The provider
  saw the same connections, stream by stream, the same peak on the account, and no probes. With no viewers it saw none.
- **The numbers.** Known viewers on two providers gave exactly the expected tries, failures by reason,
  skips, streams checked, good streams, rescues, ranks and reach, on this version. Dispatcharr's own event log agreed on the
  failures. A stream that delivered nothing got no time on air.
- **Knock-downs.** With no other plugin, comparing saved orders found the one stream that was moved down.
  With a demotion plugin, the counts per provider matched that plugin's journal, and restores matched too.
- **Privacy.** The tables and report files held no provider logins, stream URLs, user agents, IP addresses,
  user names or API keys, although the test URLs had logins in them.

**Not tested:** a real provider, real viewers over days of data, HLS and fMP4 output, XC accounts, Dispatcharr
0.34, and SQLite as Dispatcharr's own database.

## Tests

```sh
python3 -m unittest discover -s tests -t .
```

These need no server, no Django and no network. They use a real SQLite database with stand-ins for
Dispatcharr's tables, and known events whose every number can be checked by hand.

One test reads the syntax tree of every shipped file and fails on anything that could open a connection
or write to a Dispatcharr model, and on SQL writing to any table but its own.

## Licence

[MIT](LICENSE)
