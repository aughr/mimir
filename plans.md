# Partition Ring Without Kafka: Implementation Plan

## Overview

This document tracks the implementation progress of the partition ring without Kafka feature.

## Implementation Tasks

### Phase 1: Configuration Changes

| Task | Status | File(s) | Notes |
|------|--------|---------|-------|
| Add `Kafka.Enabled` field to KafkaConfig | ✅ DONE | `pkg/storage/ingest/config.go` | Default: true for backwards compatibility |
| Add `Migration.WritePercentage` field | ✅ DONE | `pkg/storage/ingest/config.go` | Range: 0-100 |
| Add `PartitionIsolationEnabled` field | ✅ DONE | `pkg/storage/ingest/config.go` | Default: false |
| Add config validation | ✅ DONE | `pkg/storage/ingest/config.go` | Added validation for all new fields |
| Add `ExpectedZoneCount` field | ⏭️ SKIPPED | N/A | Zone count determined dynamically from partition owners |

### Phase 2: Partition Ring Quorum Mode

| Task | Status | File(s) | Notes |
|------|--------|---------|-------|
| Add quorum mode config to PartitionInstanceRing | ✅ DONE | `pkg/distributor/query.go` | Implemented via `applyStrictQuorum()` |
| Implement strict quorum calculation | ✅ DONE | `pkg/distributor/query.go` | `MaxUnavailableZones = uniqueZones - ((uniqueZones / 2) + 1)` |

### Phase 3: Distributor Write Path

| Task | Status | File(s) | Notes |
|------|--------|---------|-------|
| Implement `writeToPartitionOwners()` | ✅ DONE | `pkg/distributor/distributor.go` | Direct ingester writes with zone-aware quorum |
| Add conditional routing based on `kafka.enabled` | ✅ DONE | `pkg/distributor/distributor.go` | In `sendWriteRequestToPartitions` |
| Add percentage-based routing for migration | ✅ DONE | `pkg/distributor/distributor.go` | `usePartitionRouting()` and `sendWriteRequestWithPercentageSplit()` |
| Add quorum validation before write | ✅ DONE | `pkg/distributor/distributor.go` | Fail fast if not enough healthy zones |

### Phase 4: Distributor Query Path

| Task | Status | File(s) | Notes |
|------|--------|---------|-------|
| Add query path selection | ✅ DONE | `pkg/distributor/query.go` | Based on `partition_isolation_enabled` |
| Ensure all ingesters queried during migration | ✅ DONE | `pkg/distributor/query.go` | Before cutover uses classic ring |

### Phase 5: Ingester Changes

| Task | Status | File(s) | Notes |
|------|--------|---------|-------|
| Skip Kafka reader setup when disabled | ✅ DONE | `pkg/ingester/ingester.go` | Conditionally skip Kafka reader when kafka.enabled: false |
| Verify partition lifecycler runs | ✅ DONE | N/A | Already works with existing infrastructure |

### Phase 6: Metrics

| Task | Status | File(s) | Notes |
|------|--------|---------|-------|
| Add `cortex_distributor_write_requests_total{path}` | ✅ DONE | `pkg/distributor/distributor.go` | path=classic|partition |
| Add `cortex_distributor_partition_write_latency_seconds` | ✅ DONE | `pkg/distributor/distributor.go` | Histogram of partition write latency |
| Add `cortex_partition_healthy_owners` | ✅ DONE | `pkg/distributor/distributor.go` | GaugeVec per partition, updated every 15s |
| Add `cortex_partition_state` | ✅ DONE | `pkg/distributor/distributor.go` | GaugeVec per partition, updated every 15s |
| Add `cortex_distributor_migration_write_percentage` | ✅ DONE | `pkg/distributor/distributor.go` | Current setting |

### Phase 7: Tests

| Task | Status | File(s) | Notes |
|------|--------|---------|-------|
| Write path quorum tests (`writeToPartitionOwners`) | ✅ DONE | `pkg/distributor/distributor_ingest_storage_test.go` | Added `TestDistributor_Push_WriteToPartitionOwners` |
| Read path quorum tests (`applyStrictQuorum`) | ✅ DONE | `pkg/distributor/query_test.go` | Added `TestApplyStrictQuorum` |
| Migration routing tests (`usePartitionRouting`) | ✅ DONE | `pkg/distributor/distributor_test.go` | Added `TestDistributor_usePartitionRouting` |
| Config validation tests | ✅ DONE | `pkg/storage/ingest/config_test.go` | Added tests for WritePercentage, PartitionIsolationEnabled, KafkaDisabled |
| Integration tests | ✅ DONE | `integration/partition_ring_without_kafka_test.go` | E2E tests for partition ring without Kafka |

## Progress Log

### 2025-12-06 (Session 3)

- **Completed Phase 7**: Added integration tests for partition ring without Kafka
  - Added `PartitionRingWithoutKafkaFlags` config helper to `integration/configs.go`
  - Added `TestPartitionRingWithoutKafka` - basic write/read test with partition ring
  - Added `TestPartitionRingWithoutKafkaZoneFailure` - zone failure tolerance test
  - Added `TestPartitionRingWithoutKafkaMigration` - migration from classic ring test

### 2025-12-06 (Session 2)

- **Implemented Phase 5**: Modified `pkg/ingester/ingester.go` to conditionally skip Kafka reader when `kafka.enabled: false`
- **Implemented Phase 6 (continued)**: Added `cortex_distributor_partition_write_latency_seconds` histogram metric
- **Implemented Phase 7 (partial)**: Added unit tests
  - Added `TestConfig_Validate` test cases for WritePercentage, PartitionIsolationEnabled, KafkaDisabled
  - Added `TestApplyStrictQuorum` test for read path quorum calculation
  - Added `TestDistributor_usePartitionRouting` test for percentage-based routing logic
  - Added `TestDistributor_Push_WriteToPartitionOwners` test for direct ingester writes with zone-aware quorum

### 2025-12-06 (Session 1)

- Created `design.md` with full implementation design
- Created `plans.md` for tracking progress
- Explored codebase to understand partition ring infrastructure
- **Implemented Phase 1**: Added all config fields (Kafka.Enabled, WritePercentage, PartitionIsolationEnabled)
- **Implemented Phase 2**: Added strict quorum calculation via `applyStrictQuorum()`
- **Implemented Phase 3**: Added `writeToPartitionOwners()` with zone-aware quorum, percentage-based routing
- **Implemented Phase 4**: Added query path selection based on `partition_isolation_enabled`
- **Implemented Phase 6 (partial)**: Added write path metrics and migration write percentage gauge

## Summary of Changes

### Files Modified

1. **`pkg/storage/ingest/config.go`**:
   - Added `Enabled` field to `KafkaConfig` (default: true)
   - Added `WritePercentage` field to `MigrationConfig` (default: 0)
   - Added `PartitionIsolationEnabled` field to `Config` (default: false)
   - Added validation for new fields
   - Added error variables for validation

2. **`pkg/distributor/distributor.go`**:
   - Added `writeToPartitionOwners()` function for direct ingester writes
   - Added `usePartitionRouting()` helper for percentage-based routing
   - Added `sendWriteRequestWithPercentageSplit()` for split routing during migration
   - Modified `sendWriteRequestToPartitions()` to conditionally use Kafka or direct writes
   - Modified `push()` to handle WritePercentage-based migration
   - Added metrics: `writePathRequests`, `migrationWritePercentage`, `partitionWriteLatencySeconds`

3. **`pkg/distributor/query.go`**:
   - Added `applyStrictQuorum()` helper for stricter read quorum when Kafka is disabled
   - Modified `getIngesterReplicationSetsForQuery()` to use `partition_isolation_enabled`

4. **`pkg/ingester/ingester.go`**:
   - Added conditional Kafka reader initialization: skip when `kafka.enabled: false`
   - Partition ring setup still runs regardless of Kafka setting

5. **`pkg/storage/ingest/config_test.go`**:
   - Added tests for WritePercentage validation (0, 50, 100, negative, >100)
   - Added tests for PartitionIsolationEnabled requiring WritePercentage=100
   - Added tests for Kafka disabled with/without address configured

6. **`pkg/distributor/query_test.go`**:
   - Added `TestApplyStrictQuorum` with tests for 1, 2, 3, 4, 5 zones

7. **`pkg/distributor/distributor_test.go`**:
   - Added `TestDistributor_usePartitionRouting` with comprehensive hash/percentage tests

8. **`pkg/distributor/distributor_ingest_storage_test.go`**:
   - Added `TestDistributor_Push_WriteToPartitionOwners` with zone-aware quorum tests

9. **`integration/configs.go`**:
   - Added `PartitionRingWithoutKafkaFlags` function for configuring partition ring without Kafka

10. **`integration/partition_ring_without_kafka_test.go`**:
    - Added `TestPartitionRingWithoutKafka` - basic write/read test
    - Added `TestPartitionRingWithoutKafkaZoneFailure` - zone failure tolerance test
    - Added `TestPartitionRingWithoutKafkaMigration` - migration from classic ring test

### New Files Created

1. **`design.md`**: Full implementation design document
2. **`plans.md`**: This file - implementation plan and progress tracking
3. **`design_changes.md`**: For tracking any design changes (currently empty)
4. **`integration/partition_ring_without_kafka_test.go`**: Integration tests for partition ring without Kafka

## Implementation Complete

All phases of the partition ring without Kafka implementation have been completed:

- Configuration changes for kafka.enabled, WritePercentage, and PartitionIsolationEnabled
- Zone-aware quorum for writes and reads when Kafka is disabled
- Migration support with percentage-based routing
- Comprehensive unit tests and integration tests
- Metrics for monitoring partition health and migration progress
