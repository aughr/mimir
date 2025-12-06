# Partition Ring Without Kafka: Design Changes

This document tracks any design changes discovered during implementation.

## Changes

### 1. Zone Quorum Uses ReplicationFactor Instead of Dynamic Zone Count

**Original Design (design.md lines 170-171):**
```go
expectedZones := d.cfg.ExpectedZoneCount // e.g., 3 (static config)
```

**Implemented:**
```go
replicationFactor := d.ingestersRing.ReplicationFactor()
minRequiredZones := (replicationFactor / 2) + 1
```

**Rationale:**
- Matches classic ring behavior exactly (see `ring.go` line 683: `min(len(r.ringZones), r.cfg.ReplicationFactor)`)
- No new config field needed - reuses existing ReplicationFactor
- Assumes RF = zones (standard Mimir zone-aware deployment)
- Cold start behavior: writes rejected until `(RF/2)+1` zones are available

**Behavior:**
| RF | Zones Up | minRequired | Result |
|----|----------|-------------|--------|
| 3 | 1 | 2 | REJECT (cold start) |
| 3 | 2 | 2 | Accept (no tolerance) |
| 3 | 3 | 2 | Accept (can lose 1) |

**Files Changed:**
- `pkg/distributor/distributor.go`: `writeToPartitionOwners()` uses `d.ingestersRing.ReplicationFactor()`
- `pkg/distributor/query.go`: `applyStrictQuorum()` takes `replicationFactor` parameter

### 2. ExpectedZoneCount Config Field Skipped

**Original Design:** Add `ExpectedZoneCount` config field.

**Implemented:** Skipped - using `ReplicationFactor()` instead (see above).

**Rationale:** Zone count is dynamically determined from the ingester ring, capped by RF. This matches classic ring behavior and avoids redundant configuration.
