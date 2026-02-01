# Upgrading Grafana Mimir from 2.17.x to 3.0.1

Comprehensive guide covering intentional breaking changes, subtle behavioral shifts, one-way doors, and operational hazards. Kafka ingest path changes are excluded unless they affect the classic architecture.

---

## Table of Contents

1. [One-Way Doors and Rollback Safety](#1-one-way-doors-and-rollback-safety)
2. [Architecture Changes (Must Address Before Upgrade)](#2-architecture-changes-must-address-before-upgrade)
3. [Removed CLI Flags and Configuration](#3-removed-cli-flags-and-configuration)
4. [Changed Defaults (Silent Behavioral Shifts)](#4-changed-defaults-silent-behavioral-shifts)
5. [Metric, Alert, and Dashboard Breakage](#5-metric-alert-and-dashboard-breakage)
6. [Subtle / Easy-to-Miss Changes](#6-subtle--easy-to-miss-changes)
7. [Deprecated (Still Works, Removal Coming)](#7-deprecated-still-works-removal-coming)
8. [New Capabilities Worth Knowing About](#8-new-capabilities-worth-knowing-about)
9. [Recommended Upgrade Sequence](#9-recommended-upgrade-sequence)

---

## 1. One-Way Doors and Rollback Safety

**Good news: there are no one-way doors in stored data.**

All storage formats are fully backward-compatible between 2.17 and 3.0:

| Storage Layer | Format Change? | Details |
|---|---|---|
| TSDB blocks | No | Same block meta version (TSDBVersion1), same Thanos meta version (ThanosVersion1) |
| Bucket index | No | Still IndexVersion2 |
| Alertmanager state | No | Same protobuf format, same object storage paths |
| Ruler rules | No | Same storage format and paths |
| Sparse index headers | Additive only | 3.0 compactors write optional `sparse-index-header` files alongside blocks. 2.17 store-gateways can read these. Harmless if ignored. |

**Rollback from 3.0 to 2.17 is safe from a data perspective.** However, architectural changes (query-scheduler required, Redis removed, read-write mode removed) mean your _configuration_ may not be directly revertible without preparation. Plan your rollback config before upgrading.

---

## 2. Architecture Changes (Must Address Before Upgrade)

### 2.1 Query-scheduler is now a required component

**Impact: HIGH if you run without a dedicated query-scheduler**

The embedded query-scheduler within the query-frontend has been removed. Three flags are gone:

| Removed Flag | Replacement |
|---|---|
| `-querier.frontend-address` | Use `-querier.scheduler-address` instead |
| `-querier.max-outstanding-requests-per-tenant` | Now on the scheduler: `-query-scheduler.max-outstanding-requests-per-tenant` |
| `-query-frontend.querier-forget-delay` | Now on the scheduler: `-query-scheduler.querier-forget-delay` |

**If you were running without a query-scheduler** (queriers connecting directly to query-frontends), you must:
1. Deploy a standalone query-scheduler (2+ replicas recommended)
2. Configure query-frontends: `-query-frontend.scheduler-address=<scheduler>:<port>`
3. Configure queriers: `-querier.scheduler-address=<scheduler>:<port>`

**Monolithic mode (`-target=all`) is unaffected** -- the query-scheduler is automatically included and wired up.

### 2.2 Redis cache backend removed

**Impact: HIGH if you use Redis for caching**

Redis was deprecated in 2.14 and fully removed in 3.0. All `*-<prefix>.redis.*` flags (~44 flags) are gone. The only supported remote cache backend is now **Memcached**.

If you use Redis, you must:
1. Deploy Memcached instances
2. Replace all `-<prefix>.redis.*` flags with `-<prefix>.memcached.*` equivalents
3. Accept a cold cache during the transition (no content migration possible)

Affected cache prefixes:
- `-query-frontend.results-cache.*`
- `-blocks-storage.bucket-store.index-cache.*`
- `-blocks-storage.bucket-store.chunks-cache.*`
- `-blocks-storage.bucket-store.metadata-cache.*`

### 2.3 Read-write deployment mode removed

**Impact: HIGH if you use read-write mode**

The experimental `-target=write`, `-target=read`, `-target=backend` targets are gone. You must migrate to either:
- **Microservices mode** (separate processes per component) -- recommended for production
- **Monolithic mode** (`-target=all`) -- suitable for smaller deployments

The decomposition is:
- `mimir-write` -> separate `distributor` + `ingester`
- `mimir-read` -> separate `query-frontend` + `query-scheduler` + `querier`
- `mimir-backend` -> separate `compactor` + `store-gateway` + `ruler` + `alertmanager`

### 2.4 Streaming is now mandatory

**Impact: LOW (streaming has been the default since 2.14)**

The `-ingester.stream-chunks-when-using-blocks` flag is removed. Streaming is always on.

The querier flags `-querier.streaming-chunks-per-ingester-buffer-size` and `-querier.streaming-chunks-per-store-gateway-buffer-size` now **reject zero values**. If you explicitly set either to `0`, Mimir will fail to start. Both default to `256`.

### 2.5 `-query-frontend.downstream-url` removed

**Impact: HIGH if you use the query-frontend as a proxy**

The ability to use the query-frontend to proxy requests to an arbitrary Prometheus-compatible backend is gone. If you relied on this for caching/splitting in front of external Prometheus or Thanos, you need a different solution.

---

## 3. Removed CLI Flags and Configuration

### 3.1 Fully removed flags (no replacement)

| Flag | What It Did |
|---|---|
| `-query-frontend.downstream-url` | Proxied queries to external backends |
| `-querier.frontend-address` | Connected queriers directly to query-frontends |
| `-ingester.stream-chunks-when-using-blocks` | Toggled chunk streaming (now always on) |
| `-ingester.ooo-native-histograms-ingestion-enabled` | Separate OOO native histogram toggle (now implicit) |
| `-<prefix>.memcached.addresses-provider` | Alternate DNS discovery backends for Memcached |
| All `-<prefix>.redis.*` flags | Redis cache backend |
| All instant query splitting flags | Experimental feature removed entirely |
| `service_overload_status_code_on_rate_limit_enabled` (runtime config) | HTTP 529 for rate limits (now always 429) |
| `-blocks-storage.bucket-store.index-header.eager-loading-startup-enabled` | Eager loading now always enabled with lazy loading |
| `-compactor.in-memory-tenant-meta-cache-size` | Removed experimental compactor cache |
| `-compactor.no-blocks-file-cleanup-enabled` | Cleanup now always enabled |

### 3.2 Renamed / replaced flags

| Old Flag | New Flag | Notes |
|---|---|---|
| `-query-frontend.prune-queries` | `-querier.mimir-query-engine.enable-prune-toggles` | Pruning moved into MQE |
| `-querier.max-outstanding-requests-per-tenant` | `-query-scheduler.max-outstanding-requests-per-tenant` | Moved to standalone scheduler |
| `-query-frontend.querier-forget-delay` | `-query-scheduler.querier-forget-delay` | Moved to standalone scheduler |
| Ingester reactive limiter options | `-ingester.push-reactive-limiter.*` / `-ingester.read-reactive-limiter.*` | Renamed in 3.0 |

### 3.3 Moved configuration (CLI flag name kept, YAML location changed)

The HA tracker timeout configs moved from the `distributor.ha_tracker` YAML block to the `limits` block in 2.17, and the old YAML paths were removed in 3.0:

| Old YAML Path (Removed) | New YAML Path | CLI Flag (Unchanged) |
|---|---|---|
| `distributor.ha_tracker.ha_tracker_update_timeout` | `limits_config.ha_tracker_update_timeout` | `-distributor.ha-tracker.update-timeout` |
| `distributor.ha_tracker.ha_tracker_update_timeout_jitter_max` | `limits_config.ha_tracker_update_timeout_jitter_max` | `-distributor.ha-tracker.update-timeout-jitter-max` |
| `distributor.ha_tracker.ha_tracker_failover_timeout` | `limits_config.ha_tracker_failover_timeout` | `-distributor.ha-tracker.failover-timeout` |

These are now per-tenant overridable via runtime configuration.

---

## 4. Changed Defaults (Silent Behavioral Shifts)

These won't prevent startup but will change behavior if you relied on the old defaults:

### 4.1 Query engine defaults to MQE

| Flag | Old Default | New Default |
|---|---|---|
| `-querier.query-engine` | `prometheus` | `mimir` |
| `-query-frontend.query-engine` | `prometheus` | `mimir` |

MQE is fully PromQL-compatible with transparent fallback to Prometheus engine for unsupported queries (enabled by default via `-querier.enable-query-engine-fallback=true`).

**To revert:** Set `-querier.query-engine=prometheus` and `-query-frontend.query-engine=prometheus`.

**Behavioral differences to watch for:**
- `topk`/`bottomk` may break ties differently (neither engine is deterministic for ties)
- Binary operations that produce no series may cause early cancellation of data streaming, producing `context canceled` log lines
- Per-step stats (`EnablePerStepStats`) are not supported by MQE
- Some annotations (e.g., "metric might not be a counter") may not be emitted when MQE optimizes away unnecessary evaluations

**Monitor the switch:** Watch `cortex_mimir_query_engine_supported_queries_total` and `cortex_mimir_query_engine_unsupported_queries_total` (with `reason` label) metrics.

**Per-query override:** Send the `X-Mimir-Force-Prometheus-Engine: true` header to force a specific query to use Prometheus engine.

### 4.2 HA tracker KV store defaults to memberlist

| Flag | Old Default | New Default |
|---|---|---|
| `-distributor.ha-tracker.kvstore.store` | `consul` | `memberlist` |

If you relied on the old default and were using Consul, you must either:
- Explicitly set `-distributor.ha-tracker.kvstore.store=consul`
- Or migrate to memberlist (recommended; see the [migration guide](docs/sources/mimir/configure/migrate-ha-tracker-to-memberlist.md) for a zero-downtime multi-KV-store migration process)

### 4.3 Memberlist defaults tightened (since 2.17)

| Setting | Old Default | New Default |
|---|---|---|
| `memberlist.packet-dial-timeout` | `2s` | `500ms` |
| `memberlist.packet-write-timeout` | `5s` | `500ms` |
| `memberlist.max-concurrent-writes` | `3` | `5` |
| `memberlist.acquire-writer-timeout` | `250ms` | `1s` |

These were tuned for HA tracker use and work well in low-latency networks. **In high-latency environments** (cross-region, saturated networks), the 500ms dial/write timeouts may cause dropped gossip packets. Override with the old values if needed.

### 4.4 Sparse index headers uploaded by default

| Flag | Old Default | New Default |
|---|---|---|
| `-compactor.upload-sparse-index-headers` | `false` | `true` |

This improves lazy loading startup time in store-gateways. The sparse headers are purely an optimization and are backward-compatible.

### 4.5 Store-gateway dynamic replication multiple increased

| Flag | Old Default | New Default |
|---|---|---|
| `-store-gateway.dynamic-replication.multiple` | (lower) | `5` |

Only relevant if you've enabled `-store-gateway.dynamic-replication.enabled=true`. Increases replication of recent blocks.

### 4.6 Cost attribution default cardinality reduced

| Config | Old Default | New Default |
|---|---|---|
| `max_cost_attribution_cardinality` | `5000` | `2000` |

If you use cost attribution and have high cardinality, explicitly set this to a higher value.

### 4.7 Distributor gRPC error code change

Instance-limit errors (`ERROR_CAUSE_INSTANCE_LIMIT`) now return:
- **gRPC:** `codes.Unavailable` (was `codes.Internal`)
- **HTTP:** `503 Service Unavailable` (was `500 Internal Server Error`)

Update any alerting or retry logic that keys on 500/Internal for these errors.

---

## 5. Metric, Alert, and Dashboard Breakage

### 5.1 Renamed metrics

| Old Name | New Name |
|---|---|
| `cortex_mimir_query_engine_common_subexpression_elimination_duplication_nodes_introduced` | `..._total` (added `_total` suffix) |
| `cortex_mimir_query_engine_common_subexpression_elimination_selectors_eliminated` | `..._total` |
| `cortex_mimir_query_engine_common_subexpression_elimination_selectors_inspected` | `..._total` |
| `cortex_client_request_invalid_cluster_validation_labels_total` | `cortex_client_invalid_cluster_validation_label_requests_total` |
| `cortex_ingest_storage_writer_produce_requests_total` | `cortex_ingest_storage_writer_produce_records_enqueued_total` |
| `cortex_ingest_storage_writer_produce_failures_total` | `cortex_ingest_storage_writer_produce_records_failed_total` |

### 5.2 Removed metrics

| Metric | Version Removed |
|---|---|
| `cortex_distributor_label_values_with_newlines_total` | 2.17.0 |
| `cortex_blockbuilder_process_partition_duration_seconds` | 3.0.0 |

### 5.3 New labels on existing metrics (changes cardinality)

| Metric | New Label | Values | Impact |
|---|---|---|---|
| `cortex_distributor_uncompressed_request_body_size_bytes` | `handler` | `push`, `otlp` | Series split by handler; queries without `by(handler)` still aggregate correctly |
| `cortex_prometheus_rule_evaluation_failures_total` | `reason` | `user`, `operator` | Useful for separating user errors from infra failures |
| `cortex_distributor_requests_in_total` | `version` | `1.0`, `2.0` | Remote Write protocol version |
| `cortex_ingest_storage_writer_latency_seconds` | `outcome` | `success`, `failure` | Previously only tracked successes; add `{outcome="success"}` to maintain old view |

### 5.4 Alert changes

| Alert | Change |
|---|---|
| `MimirFrontendQueriesStuck` | **Removed** (query-scheduler is required; alert no longer applicable) |
| `MimirAlertmanagerInitialSyncFailed` | Severity changed from `critical` to `warning` |
| `BlockBuilderLagging` | Replaced by `MimirBlockBuilderSchedulerPendingJobs` |

### 5.5 Overrides-exporter behavior change (2.17)

The overrides-exporter **no longer emits `cortex_limits_overrides` for tenants whose limits match the default**. If your dashboards query `cortex_limits_overrides{user="tenant-a"}` for a tenant at default values, those series will be missing.

**Fix:** Use a fallback pattern:
```promql
cortex_limits_overrides{limit_name="X", user="tenant-a"}
  or
cortex_limits_defaults{limit_name="X"}
```

---

## 6. Subtle / Easy-to-Miss Changes

### 6.1 Memcached DNS: search domains no longer work

**This is the single most operationally dangerous subtle change.**

The Memcached DNS resolver switched from Go's standard `net.Resolver` to `miekgdns`. The new resolver:
- Does **NOT** support DNS search domains
- Does **NOT** support `ndots`
- Only uses TCP connections to nameservers

**If you configure Memcached addresses as short names** (e.g., `dns+memcached-frontend:11211` relying on Kubernetes search domains), resolution will fail with NXDOMAIN.

**Fix:** Use fully-qualified domain names everywhere:
```
dns+memcached-frontend.mynamespace.svc.cluster.local:11211
```

This also affects `-memberlist.join` addresses and `-ruler.alertmanager-url`.

### 6.2 Out-of-order native histograms are now always enabled

If both native histogram ingestion AND out-of-order ingestion (`-ingester.out-of-order-time-window > 0`) are enabled, out-of-order native histograms are automatically accepted. There is no longer a separate toggle. The old `-ingester.ooo-native-histograms-ingestion-enabled` flag is removed.

If you previously had this set to `false` to prevent OOO native histograms while allowing OOO float samples, that restriction is gone.

### 6.3 Cost attribution config rename (2.17)

`max_cost_attribution_cardinality_per_user` was renamed to `max_cost_attribution_cardinality`. The old name will cause startup failure.

### 6.4 Query-frontend instant query splitting removed

The experimental `-query-frontend.split-instant-queries-by-interval` feature is completely removed. If you had this enabled, instant queries will no longer be split.

### 6.5 Admin UI uses relative links

The administration web UI (`/admin/`) now uses relative links instead of absolute ones. If you have a reverse proxy that strips path prefixes, links may break.

### 6.6 Alertmanager InitialSyncFailed severity downgraded

Changed from `critical` to `warning`. If you route alerts by severity for paging, this alert will no longer page.

### 6.7 Querier and query-frontend: per-step stats removed with MQE

Per-step query statistics are no longer supported when MQE is the active engine. The `-query-frontend.cache-samples-processed-stats` flag is deprecated and has no effect.

### 6.8 Distributor: rate-limiting always returns HTTP 429

The experimental `service_overload_status_code_on_rate_limit_enabled` setting (which returned HTTP 529 instead of 429) is removed. If clients or load balancers were configured for 529, update them to expect 429.

---

## 7. Deprecated (Still Works, Removal Coming)

These are deprecated in 3.0 and will be removed in a future release. Plan to migrate:

| Deprecated Item | Replacement |
|---|---|
| Consul for HA tracker (`-distributor.ha-tracker.kvstore.store=consul`) | Use `memberlist` |
| etcd for HA tracker (`-distributor.ha-tracker.kvstore.store=etcd`) | Use `memberlist` |
| `-distributor.otel-start-time-quiet-zero` | Native OTel start time conversion is now default |
| `-store-gateway.sharding-ring.auto-forget-enabled` | Use `-store-gateway.sharding-ring.auto-forget-unhealthy-periods=0` to disable |
| `-ruler.alertmanager-url` / `alertmanager_client` | Use per-tenant `ruler_alertmanager_client_config` |
| `-query-frontend.cache-samples-processed-stats` | No replacement (feature removed with MQE) |

---

## 8. New Capabilities Worth Knowing About

While not required for the upgrade, these are notable additions:

- **MQE per-query memory limits:** `-querier.max-estimated-memory-consumption-per-query` (MQE-only, not available with Prometheus engine)
- **MQE optimization flags:** CSE for range vectors, prune toggles, histogram decoding skip, narrow binary selectors, reduce matchers
- **MQE remote execution** (experimental): `-query-frontend.enable-remote-execution` for streaming query execution in queriers
- **Zone-aware querying:** `-querier.prefer-availability-zones` to prefer specific zones for ingester/store-gateway queries
- **GCS upload retries:** `-blocks-storage.gcs.enable-upload-retries` and `-blocks-storage.gcs.max-retries`
- **Microsoft Teams V2** Alertmanager integration
- **Remote-Write 2.0** (experimental): Protocol auto-detected via `cortex_distributor_requests_in_total{version="2.0"}`
- **Ruler improvements:** Rule group level labels now supported; per-tenant alertmanager client config; max rule evaluation results limit

---

## 9. Recommended Upgrade Sequence

### Pre-upgrade checklist

1. **Audit your configuration** for all removed flags listed in Section 3. Mimir 3.0 will fail to start if any removed flag is present.

2. **Deploy a query-scheduler** if you don't already have one (Section 2.1).

3. **Migrate from Redis to Memcached** if applicable (Section 2.2). Do this on 2.17 first.

4. **Migrate from read-write deployment mode** if applicable (Section 2.3).

5. **Fully qualify all DNS names** used in Memcached addresses, memberlist join, and alertmanager URLs (Section 6.1). This is safe to do on 2.17 first.

6. **Migrate HA tracker to memberlist** if using Consul/etcd (Section 4.2). Use the multi-KV-store migration process for zero downtime. This can be done on 2.17 first.

7. **Update dashboards and alerts** per Section 5. Remove references to deleted metrics and alerts. Update queries affected by new labels.

8. **Review changed defaults** (Section 4). Decide whether to explicitly set old values or accept new behavior.

### Upgrade order

For microservices deployments, upgrade in this order to minimize risk:

1. **Compactors** -- No query-path impact. Validates block storage compatibility.
2. **Store-gateways** -- Will benefit from sparse index headers. Roll carefully.
3. **Query-scheduler** (deploy if new) -- Must be running before upgrading query-frontends.
4. **Query-frontends** -- Includes MQE switch. Monitor fallback metrics.
5. **Queriers** -- Will connect to scheduler instead of frontend if previously direct.
6. **Distributors** -- HA tracker default changes take effect here.
7. **Ingesters** -- Roll last. Most stateful; most impactful if something goes wrong.
8. **Rulers** -- After queriers are stable on MQE.
9. **Alertmanagers** -- Low risk for this upgrade.

### Post-upgrade validation

1. Verify `cortex_mimir_query_engine_supported_queries_total` is incrementing (MQE is working)
2. Check `cortex_mimir_query_engine_unsupported_queries_total` for fallback rate
3. Confirm no `unknown flag` errors in logs
4. Verify Memcached connectivity (especially if DNS names changed)
5. Confirm HA tracker is functioning if KV store changed
6. Validate dashboards show data for renamed/re-labeled metrics
