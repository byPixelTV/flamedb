# FlameDB Grafana datasource

Visual query builder for FlameDB's TCP protocol. Supports metric discovery, equality tag filters, AVG/SUM/COUNT time buckets, a series per tag combination, latest-value Stat panels and joined latest-value tables. Grafana handles units, thresholds, panel layout and sparklines. No raw query syntax is required.

## Build on Windows

Requires Node.js 22+, Go 1.26.5+, and Grafana 13.2+. From this directory:

```powershell
npm.cmd ci
npm.cmd run build
```

The `dist` directory contains the frontend and backend binaries for Windows amd64, Linux amd64 and Linux arm64. Copy its contents into a Grafana plugin directory named `flamedb-flamedb-datasource`. On Linux, make the matching `gpx_flamedb_linux_*` backend executable (`chmod +x`). Restart Grafana with this unsigned development plugin enabled:

```ini
[plugins]
allow_loading_unsigned_plugins = flamedb-flamedb-datasource
```

You can also set `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=flamedb-flamedb-datasource`. Signing and publication to the Grafana plugin catalog are separate steps. Grafana Cloud requires an approved installation path; this local unsigned plugin is intended for self-hosted Grafana.

## Native Windows demo

```powershell
.\scripts\setup-grafana.ps1
npm.cmd run demo
```

This downloads the pinned official Grafana Windows amd64 archive, verifies its SHA256, builds an isolated FlameDB instance and opens no installers or services. The script prints the demo dashboard URL. It uses synthetic metrics and a dedicated read-only datasource key. Its generated session record contains the two process IDs; stop those processes when finished. Other platforms can set `GRAFANA_HOME` to an existing Grafana distribution and run `npm run demo`.

## Configure

Add the FlameDB data source in Connections. Enter the FlameDB host reachable **from the Grafana server**, TCP port (default 7777), and a dedicated API key with `read` permission. Grafana stores the key in `secureJsonData`; it is never included in the query model. Save & Test authenticates and runs `METRICS`.

For Grafana in Docker on Windows, use `host.docker.internal` to reach a FlameDB server on the host; FlameDB must listen on an address accessible from Docker. The plugin currently supports unencrypted TCP connections only. FlameDB's optional TLS listener is not supported in this version.

```powershell
$env:FLAMEDB_HOST = 'host.docker.internal'
$env:FLAMEDB_PORT = '7777'
$env:FLAMEDB_API_KEY = '<your read-only key>'
docker compose up -d
```

This exposes Grafana on http://localhost:3300 and provisions the datasource and example dashboard. Grafana uses its initial admin login; change it on first login. The compose service binds only to localhost.

## Build a panel

1. Choose metrics, for example `tps` and `players`.
2. Add tag filters such as `server = survival-1`. Filters are exact matches joined with AND.
3. Choose **Time series**, **Latest value per group**, or **Table: latest values per group**.
4. For time series, choose Average, Sum or Count events and an automatic or explicit interval.
5. Optionally group by `server`, `world`, or `region`. Use the Table panel for joined region rows, or a Stat panel with a sparkline for time series.
6. Set panel units, colors and thresholds in Grafana. Legend templates support `{{metric}}` and tags such as `{{server}}`.

The dashboard time range always applies. Latest means the last sample **inside that range**, not necessarily a current live value. The table includes a timestamp column per metric so different sample ages remain visible. Missing observations are null, not zero. A grouped time series tolerates one empty bucket for sampling jitter and inserts a null for longer gaps. This does not invent missing samples or fill long outages.

Metric names come from FlameDB's cluster-wide `METRICS` command. Tag suggestions are a bounded sample of the latest 1,000 events of the first selected metric; suggestions may be incomplete. You can enter custom metric names, tag names and values. Dashboard variables can be used in metric names, filter values and legends; only single-value exact filters are supported. Dynamic query-variable discovery and multi-value/All expansion are not implemented.

With an updated FlameDB server, tag-grouped histories and latest tables are computed on the server. The server streams matching events without retaining or returning the raw history. These queries do **not** use the plugin's 50,000-event raw scan limit. Series output is automatically coarsened when needed (at most 20,000 points across the panel's metrics); sums/counts include every event and AVG uses weighted sums/counts. No part of the dashboard time range is dropped. Latest mode returns one compact observation per tag combination and metric.

Grouped output remains bounded: up to 1,000 groups for time series and latest Stat panels, 10,000 table groups and a bounded response size. If a long range contains more unique historical region IDs, filter by server/world/region. The entire query still obeys the configured timeout; increase it for genuinely expensive histories if needed. Output limits are explicit errors, not silent truncation.

Update **both FlameDB and the datasource plugin** for this path; in a cluster update every node that serves reads. No dashboard JSON change is needed. An older server automatically falls back to bounded raw time-window reads, where Scan limit per metric still applies. The fallback cannot provide unlimited history and its error tells you to update the server. Time series without tag grouping continue using native FlameDB aggregation. Raw responses remain bounded to 16 MiB, connections to 8 per datasource, and a panel query defaults to a 10-second timeout. Pagination uses separate storage snapshots, so the legacy fallback is not an atomic snapshot of concurrent historical backfills.

The new protocol is single-metric `GET ... GROUP BY 1m AVG TAGS server,world,region POINTS 20000` for series and `GET ... TAGS server,world,region LAST` for latest values, with the usual WHERE/FROM/TO filters. POINTS caps total output points; the response reports its effective `interval_ns`. Fixed-duration buckets are supported; calendar month/year grouping is intentionally rejected for this path.

Alerts use the same backend queries; template variables in alert rules should be replaced by literal metric/filter values. For the overview template, write `tps`, `mspt`, `players`, `cpu_load`, `memory_used` and `chunks` metrics with `server`, `world` and `region` tags. Rename the metrics in the builder to match your collector. CPU is expected in percent and memory in bytes; adjust units for your collector.

## Verify

```powershell
go test ./pkg/... -count=1
npm.cmd run build
```

`scripts/integration.mjs` requires a portable Grafana distribution under `.runtime` (or `GRAFANA_HOME`) and builds a fresh FlameDB binary from the parent repository. It starts temporary loopback-only services on free ports, seeds synthetic data, checks Grafana's actual plugin health/resource/query endpoints and removes its processes on completion. Set `KEEP_GRAFANA_TEST=1` to keep the demo running for manual inspection; its generated URLs and process IDs are saved under `.runtime/session.json`. The isolated test datasource uses synthetic test keys and temporary storage, never existing FlameDB data directories.


