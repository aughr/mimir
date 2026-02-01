# Upgrade guide: Mimir 2.17.x → 3.0.1

This document covers every breaking change, silent default shift, one-way door,
and subtle gotcha when upgrading from Mimir 2.17.x to 3.0.1.  It is organised
from "will crash your cluster if you miss it" down to "worth knowing about."

> **Kafka / ingest-storage scope.**  This guide assumes you are **not** using the
> Kafka ingest path.  Where ingest-storage is relevant to classic-mode operators
> (e.g. the Helm chart now defaults to ingest-storage ON) those sections are
> called out explicitly.

---

## 1  One-way doors

These changes cannot be undone by a simple rollback.  Understand them before
you begin.

### 1.1  Streaming is now mandatory between querier ↔ ingester ↔ store-gateway

The non-streaming series query path in the store-gateway has been **deleted**.
A querier built against 3.0.x will always set a non-zero `StreamingChunksBatchSize`
and a store-gateway built against 3.0.x will reject (or mishandle) the old
batch-size-0 non-streaming requests.

* `-querier.streaming-chunks-per-ingester-buffer-size` and
  `-querier.streaming-chunks-per-store-gateway-buffer-size` are validated to be
  non-zero at startup.  Setting either to `0` is now a hard error.
* This is consistent with streaming having been the default since 2.14.  If you
  explicitly set either flag to `0` anywhere in your config, remove those lines.

**Rolling-upgrade ordering:** upgrade queriers *before* store-gateways, or
upgrade all query-path components together.  An old querier talking to a new
store-gateway will receive streaming responses it does not expect.

### 1.2  Query-scheduler is a required, dedicated component

The query-frontend can no longer embed its own scheduler.  You **must** run a
separate query-scheduler deployment.

Removed flags (setting them causes an unknown-flag startup error):

| Removed flag | What it did |
|---|---|
| `-querier.frontend-address` | Connected querier directly to frontend (bypassing scheduler) |
| `-querier.max-outstanding-requests-per-tenant` | Was on the frontend; now lives on the scheduler only |
| `-query-frontend.querier-forget-delay` | Was on the frontend; now lives on the scheduler only |

The equivalent flags on the query-scheduler (`-query-scheduler.max-outstanding-requests-per-tenant`,
`-query-scheduler.querier-forget-delay`) remain.

### 1.3  Sparse index header upload failure now blocks the entire block upload

Previously, uploading the sparse index header after a compaction was
best-effort: a failure was logged as a warning and the block upload succeeded
anyway.  In 3.0.x the sparse header upload runs **concurrently** with the
main block upload inside the same error-group.  If it fails, the entire
block upload is rolled back.

* The default of `-compactor.upload-sparse-index-headers` is now `true`.
* A transient object-storage error on the sparse header will prevent that
  compaction job from completing.
* Downgrading back to 2.17.x after uploading sparse headers is safe (they are
  simply ignored), but an upgrade to 3.0.x makes sparse-header failures fatal
  for compaction.

### 1.4  Store-gateway dynamic replication default: 3 → 5

`-store-gateway.dynamic-replication.multiple` defaults to `5` (was `3`).  This
means recent blocks are replicated at 5× the base replication factor.  After
upgrade your store-gateways will sync significantly more data until the blocks
age out of the "recent" window.

If you want to preserve the old behaviour, explicitly set the flag to its
previous value before upgrading:

```
-store-gateway.dynamic-replication.multiple=3
```

---

## 2  Things that will crash Mimir on startup

Mimir's runtime-config and flag parsers use **strict mode**.  Any unknown flag
or unknown YAML key in a per-tenant override file is a hard startup failure,
not a silent ignore.

### 2.1  Removed CLI flags

Remove every occurrence of these flags from your flag files, command-line
arguments, and Helm `args` overrides:

| Flag | Replacement / action |
|---|---|
| `-query-frontend.downstream-url` | Remove.  Proxy mode is gone; use the scheduler. |
| `-query-frontend.prune-queries` | Replace with `-querier.mimir-query-engine.enable-prune-toggles` (default `true`) |
| `-querier.frontend-address` | Remove (see §1.2) |
| `-querier.max-outstanding-requests-per-tenant` | Move to `-query-scheduler.max-outstanding-requests-per-tenant` |
| `-query-frontend.querier-forget-delay` | Move to `-query-scheduler.querier-forget-delay` |
| `-ingester.stream-chunks-when-using-blocks` | Remove.  Streaming is mandatory. |
| `-<prefix>.memcached.addresses-provider` | Remove.  The reliable SD backend from 2.16 is now the only one. |
| `service_overload_status_code_on_rate_limit_enabled` | Remove.  HTTP 429 is always used now. |

### 2.2  Redis cache backend removed

Redis is no longer a supported cache backend.  Only Memcached remains.
Any `-*.redis.*` configuration will be rejected.  **Migrate to Memcached
before upgrading.**

### 2.3  Removed per-tenant override keys

Because the YAML decoder enforces `KnownFields: true`, any of the following
keys in your per-tenant override YAML will prevent Mimir from starting:

| Key to remove | Why |
|---|---|
| `ingester_stream_chunks_when_using_blocks` | Runtime option removed; streaming is mandatory |
| `service_overload_status_code_on_rate_limit_enabled` | Experimental distributor setting removed |
| `ooo_native_histograms_ingestion_enabled` | Always on; removed in 2.17.0 (if you upgraded from <2.17 and missed it) |
| `max_cost_attribution_cardinality_per_user` | Renamed to `max_cost_attribution_cardinality` in 2.17.0 |
| `max_cost_attribution_labels_per_user` | Removed in 2.17.0 |
| `ingestion_artificial_delay_condition_for_tenants_with_less_than_max_series` | Hidden experimental; removed between 3.0.0 and 3.0.1 |
| `ingestion_artificial_delay_duration_for_tenants_with_less_than_max_series` | Same |
| `ingestion_artificial_delay_condition_for_tenants_with_id_greater_than` | Same |
| `ingestion_artificial_delay_duration_for_tenants_with_id_greater_than` | Same |

### 2.4  Removed global HA-tracker timeout flags

The following flags on the distributor were removed (they were deprecated
in 2.17.0 and fully deleted in 3.0.0):

* `distributor.ha_tracker.ha_tracker_update_timeout`
* `distributor.ha_tracker.ha_tracker_update_timeout_jitter_max`
* `distributor.ha_tracker.ha_tracker_failover_timeout`

The per-tenant equivalents (`ha_tracker_update_timeout`,
`ha_tracker_update_timeout_jitter_max`, `ha_tracker_failover_timeout` in the
limits block) are the only way to set these now.

### 2.5  Read-write deployment mode removed

The experimental read-write deployment mode and all associated module targets
have been removed.  If you are using it, you must migrate to the standard
microservices deployment before upgrading.

### 2.6  Instant query splitting removed

The experimental instant-query-splitting feature was removed entirely.  No
flag remains; no action is needed unless you were explicitly referencing it
in documentation or tooling.

---

## 3  Silent default changes

These will not crash Mimir, but they change behaviour if you don't pin the
old value explicitly.

### 3.1  Mimir Query Engine (MQE) is now the default at the query-frontend

| Flag | Old default | New default |
|---|---|---|
| `-query-frontend.query-engine` | `prometheus` | `mimir` |
| `-querier.query-engine` | Already `mimir` since 2.17 | `mimir` |

MQE is the only engine that supports the new query-planning and pruning
features.  If you need to temporarily fall back, set
`-query-frontend.query-engine=prometheus`, but be aware that pruning and
several optimisations will be disabled.

### 3.2  HA tracker defaults to memberlist

`-distributor.ha-tracker.kvstore.store` now defaults to `memberlist`.  If your
deployment was implicitly using consul or etcd for the HA tracker (i.e. you
never set this flag), the HA tracker will silently switch to memberlist on
upgrade.

**If memberlist / the gossip ring is not already configured, HA tracking will
break silently.**  Either:

* Verify your gossip ring is healthy and all distributor pods can reach each
  other on the memberlist port, **or**
* Explicitly set `-distributor.ha-tracker.kvstore.store=consul` (or `etcd`) to
  preserve the old behaviour (both are deprecated but still functional).

### 3.3  Cost-attribution cardinality default halved

`-validation.max-cost-attribution-cardinality` default dropped from 5000 to
2000.  Tenants with more than 2000 unique cost-attribution label combinations
will see overflow after upgrade (tracked via
`cortex_attributed_series_overflow_labels`).

Pin the old value explicitly if you need time to investigate:

```
-validation.max-cost-attribution-cardinality=5000
```

### 3.4  Labels query optimizer enabled by default

`-query-frontend.labels-query-optimizer-enabled` flipped from `false` to
`true`.  This is a per-tenant override (`labels_query_optimizer_enabled`).  If
the optimiser causes unexpected results on any tenant, you can disable it
per-tenant without a restart.

### 3.5  Alertmanager InitialSyncFailed severity downgraded

The `InitialSyncFailed` alert severity changed from `critical` to `warning`.
Update your alerting rules or PagerDuty routing if you were routing on
severity.

---

## 4  Write-path changes

### 4.1  Instance-limit errors return HTTP 503 instead of 500

gRPC errors with cause `ERROR_CAUSE_INSTANCE_LIMIT` (e.g. distributor or
ingester at max inflight requests) now map to `codes.Unavailable` / HTTP 503
instead of `codes.Internal` / HTTP 500.

**Impact:** any upstream client or proxy (including Prometheus remote-write
retry logic) that retries only on specific status codes needs to include 503
in its retry set.  Prometheus already retries on 503, so standard
remote-write clients are fine.

### 4.2  HTTP 529 rate-limit status removed

The non-standard HTTP 529 status code for rate limiting is gone.  All
rate-limit responses now consistently return HTTP 429.  The flag
`service_overload_status_code_on_rate_limit_enabled` no longer exists (see §2.1).

### 4.3  New experimental label-value strategies (opt-in)

Two new experimental distributor flags control how oversized label values are
handled:

* `-validation.label-value-length-over-limit-strategy` — values: `error`
  (default, unchanged behaviour), `truncate`, `drop`.
* `-validation.name-validation-scheme` — values: `legacy` (default),
  `utf8`.

**Do not enable these without understanding the per-tenant implications.**
`truncate` and `drop` will silently mutate or discard label data that was
previously rejected outright.  `utf8` will accept metric and label names that
`legacy` rejects.

### 4.4  OTLP start-time handling rewritten

The `-distributor.otel-start-time-quiet-zero` flag is deprecated.  The OTLP
endpoint now natively converts OpenTelemetry start times to Prometheus
created timestamps using proper semantics rather than the old QuietZeroNaN
approach.  No action is required unless you were explicitly setting this flag;
if so, remove it.

---

## 5  Query-path and API changes

### 5.1  Queriers no longer expose a Prometheus HTTP API

The Prometheus HTTP API (`/api/v1/query`, `/api/v1/query_range`, etc.) is now
served **only** by the query-frontend.  Do not route external traffic directly
to queriers.

### 5.2  Cardinality and active-series endpoints now stable

`/api/v1/cardinality/active_series` and `/api/v1/user_limits` graduated from
experimental to stable.  No action needed, but you can remove any
`experimental` annotations from your documentation.

### 5.3  UTF-8 support in cardinality endpoints and PromQL functions

`/api/v1/cardinality/{label_names,label_values,active_series}` and the PromQL
functions `label_join`, `label_replace`, and `count_values` now accept and
return UTF-8 label and metric names.  This change is gated by the per-tenant
`name_validation_scheme` setting (§4.3).

### 5.4  Three MQE metrics renamed (added `_total` suffix)

| Old name | New name |
|---|---|
| `cortex_mimir_query_engine_common_subexpression_elimination_duplication_nodes_introduced` | …`_total` |
| `cortex_mimir_query_engine_common_subexpression_elimination_selectors_eliminated` | …`_total` |
| `cortex_mimir_query_engine_common_subexpression_elimination_selectors_inspected` | …`_total` |

Update any dashboards or recording rules that reference these metrics.

### 5.5  Deterministic sample merging

When two samples share the same timestamp (e.g. from an out-of-order ingest),
the merge is now deterministic.  Previously this could cause query results to
flap.  This is a pure bug fix with no configuration impact.

---

## 6  Limits and runtime overrides

### 6.1  Override validation is strict — no silent ignoring

Both the top-level runtime-config YAML decoder and the per-tenant `Limits`
YAML/JSON decoders enforce strict field checking.  Any stale or misspelled
key in your override file is a hard startup failure.  See §2.3 for the
exhaustive list of removed keys.

### 6.2  New per-tenant fields available

These fields can now appear in per-tenant override YAML.  They did not exist in
2.17.0 and have no effect unless explicitly set:

| Key | Purpose |
|---|---|
| `label_value_length_over_limit_strategy` | `error` / `truncate` / `drop` |
| `name_validation_scheme` | `legacy` / `utf8` |
| `ha_tracker_sample_failover_timeout` | Sample-time-based HA failover (default 0 = off) |
| `ingestion_burst_factor` | Express burst as a multiple of rate limit |
| `ruler_evaluation_consistency_max_delay` | Max ingestion delay for consistent ruler evals (ingest-storage only) |
| `ruler_max_rule_evaluation_results` | Cap on alerts/series per rule |
| `labels_query_optimizer_enabled` | Per-tenant labels-query optimiser toggle |

### 6.3  Overrides-exporter auto-discovers fields

The overrides-exporter no longer uses a hardcoded metric list.  It uses
reflection on YAML struct tags.  Any numeric, boolean, or `model.Duration` field
in the `Limits` struct can be exported by adding its YAML tag name to
`-overrides-exporter.enabled-metrics`.  Metric names must match YAML tags
exactly.

---

## 7  Rings and service discovery

### 7.1  New querier ring

A new ring (`querier`) was introduced for remote-execution service discovery.
Queriers register themselves with a single token.  The query-frontend reads
this ring to determine which queriers are available and what query-plan version
they support.

* Ring key: `querier`
* Flag prefix: `querier.ring.*`
* Default heartbeat: 15s period, 1m timeout
* Queriers unregister on clean shutdown (`KeepInstanceInTheRingOnShutdown: false`)

This ring is inert unless remote execution (`-query-frontend.enable-remote-execution`)
is enabled.  No action is needed if you are not using that feature, but the
ring flags are now registered and will appear in flag listings.

### 7.2  Store-gateway auto-forget-enabled deprecated

`-store-gateway.sharding-ring.auto-forget-enabled` is tagged `deprecated`.
Use `-store-gateway.sharding-ring.auto-forget-unhealthy-periods` instead
(set to `0` to disable).  The flag still works but will be removed in a
future release.

### 7.3  Etcd-operator removed from Jsonnet

The Jsonnet configuration no longer deploys or manages etcd.  If you use etcd
for any ring KV store you must now deploy and operate it yourself.  Combined
with the HA tracker defaulting to memberlist (§3.2), this is a strong signal
that memberlist is the expected ring backend going forward.

---

## 8  Ruler

### 8.1  Rule-group proto extended: labels and limit fields

Two new fields (`labels`, `limit`) were added to the `RuleGroupDesc` protobuf
message.  These correspond to fields Prometheus already supports at the rule-
group level but that Mimir was previously silently ignoring.

* **Backward-compatible for reads:** existing rule groups stored without these
  fields deserialise with zero values (empty labels, limit 0 = no limit), which
  matches previous behaviour.
* **Forward-incompatible for writes:** if you write a rule group that includes
  labels or a limit via the 3.0.x API and then roll back to 2.17.x, the
  2.17.x ruler will silently ignore those fields.

### 8.2  New remote evaluation transport

The ruler gained an HTTP-based transport option for remote rule evaluation
(alongside the existing gRPC path).  This is opt-in via configuration and does
not affect existing deployments.

### 8.3  Per-tenant `ruler_max_rule_evaluation_results`

A new per-tenant limit caps the number of alerts an alerting rule or series a
recording rule can produce.  Defaults to 0 (no limit).  No action needed
unless you want to use it.

---

## 9  Monitoring and alerting changes

### 9.1  `MimirFrontendQueriesStuck` alert removed from mixin

This alert is no longer relevant because the query-scheduler is now required.
If you have imported the mixin and are routing on this alert name, remove or
update the routing rule.

### 9.2  Block-builder metric removed

`cortex_blockbuilder_process_partition_duration_seconds` was removed.  Update
any dashboards or recording rules that reference it.

### 9.3  Distributor metric gains handler label

`cortex_distributor_uncompressed_request_body_size_bytes` now has a `handler`
label differentiating the source of the request.  Existing dashboards that
`sum()` this metric without grouping are unaffected; those that group by all
labels will see the cardinality increase.

---

## 10  Helm chart (5.8.0 → 6.0.4)

The Helm chart jumped a major version.  The following items are relevant to
operators **not** using Kafka ingest-storage.

### 10.1  Ingest-storage is ON by default — you must explicitly opt out

Chart 6.0.0 ships with `ingest_storage.enabled: true` and deploys a
single-node Kafka StatefulSet.  To keep the classic write path:

```yaml
mimir:
  structuredConfig:
    ingest_storage:
      enabled: false
    ingester:
      push_grpc_method_enabled: true
kafka:
  enabled: false
```

Without this override the chart will deploy Kafka and wire ingest-storage
unconditionally.

### 10.2  Minimum Kubernetes version raised to 1.29

`kubeVersion: ^1.29.0-0` in `Chart.yaml`.  Clusters below 1.29 will fail
Helm validation.

### 10.3  Top-level `nginx` values removed

If your `values.yaml` has a top-level `nginx:` block it must be migrated to
`gateway:` before upgrading (this migration path has been available since
chart 5.6.0).

### 10.4  Rollout-operator CRDs must be installed manually

Before running `helm upgrade`, apply:

```bash
kubectl apply -f https://raw.githubusercontent.com/grafana/helm-charts/main/charts/rollout-operator/crds/replica-templates-custom-resource-definition.yaml
kubectl apply -f https://raw.githubusercontent.com/grafana/helm-charts/main/charts/rollout-operator/crds/zone-aware-pod-disruption-budget-custom-resource-definition.yaml
```

If upgrading from chart 6.0.0 or 6.0.1, also delete the rollout-operator
`certificate` secret and restart the rollout-operator pod (TLS cert DNS name
fix in rollout-operator 0.37.1).

### 10.5  Query-scheduler always required

`frontend.scheduler_address` and `frontend_worker.scheduler_address` in the
rendered config always point to the query-scheduler headless service.  Do not
disable or remove the query-scheduler deployment.

### 10.6  Image tag default mechanism changed

The chart now derives the default image tag from `appVersion` in `Chart.yaml`
rather than from an explicit `image.tag` field.  If you pinned `image.tag` in
your values, that override still works.  If you relied on the field being
present and empty, update your tooling.

### 10.7  GEM (enterprise) values removed

All `enterprise.*` values have been stripped.  Any such values in your
`values.yaml` will be silently ignored.

### 10.8  `metaMonitoring.grafanaAgent` deprecated

Grafana Agent reached end-of-support.  Migrate to an external collector
(e.g. Grafana k8s-monitoring / Alloy).

---

## 11  Pre-upgrade checklist

Run through this list before upgrading any component:

1. **Grep your config for removed flags** (§2.1).  Every flag in that table
   will cause an unknown-flag startup error.
2. **Grep your per-tenant override YAML for removed keys** (§2.3).  Every key
   in that table will prevent runtime-config loading.
3. **Migrate away from Redis** if you use it as a cache backend (§2.2).
4. **Verify your gossip ring** is healthy if you rely on the HA tracker without
   an explicit KV store setting (§3.2).
5. **Pin defaults you want to keep** before upgrading: dynamic-replication
   multiple (§1.4), cost-attribution cardinality (§3.3), query engine (§3.1).
6. **Plan rolling-upgrade order** for the query path: queriers before
   store-gateways (§1.1).
7. **Deploy or verify a dedicated query-scheduler** (§1.2).
8. **Update dashboards and alerts** for renamed metrics (§5.4, §9.1, §9.2).
9. **For Helm:** opt out of ingest-storage if not using it (§10.1), apply
   rollout-operator CRDs (§10.4), verify K8s ≥ 1.29 (§10.2).
10. **Update client retry logic** if you were relying on HTTP 500 for
    instance-limit errors; they are now HTTP 503 (§4.1).
