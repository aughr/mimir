# Partition Ring Without Kafka: Implementation Design

## Overview

This document describes a design to enable Mimir's partition ring infrastructure to work without Kafka, providing per-partition failure isolation while maintaining the simpler operational model of the classic architecture.

The core insight is that the partition ring already provides all the mechanisms we need for per-partition failure isolation - we just need to swap out the Kafka transport for direct ingester writes.

**Note**: This is a design proposal. Configuration fields like `kafka.enabled`, `write_percentage`, and `partition_isolation_enabled`, as well as functions like `writeToPartitionOwners()`, are **new additions proposed by this design**, not existing code. Implementation details are left to the implementer; this document focuses on intent and requirements.

## Problem Statement

Mimir's **classic architecture** suffers from **zone contamination**: a single unhealthy ingester marks its entire zone as failed. With RF=3 across 3 zones:
- 1 failure in zone-a + 1 failure in zone-b = **100% query failure**
- This is mathematically catastrophic at scale

**Ingest Storage** solves this with per-partition isolation, but requires Kafka:
- Each partition is evaluated independently
- A partition fails only when ALL its owners are unhealthy
- This dramatically improves failure tolerance

**Goal**: Get per-partition failure isolation without requiring Kafka infrastructure.

## Solution: Reuse Partition Ring, Skip Kafka

The partition ring infrastructure already handles:
- Partition state machine (PENDING → ACTIVE → INACTIVE → DELETED)
- Explicit ownership tracking with timestamps
- Token-based series routing via consistent hashing
- Shuffle sharding with lookback
- Per-partition failure isolation in queries
- Gossip-safe merging via memberlist

We reuse all of this, but instead of writing to Kafka partitions, we write directly to partition owners (ingesters).

### Architecture Comparison

| Aspect | Classic | Ingest Storage (Kafka) | Partition Ring (No Kafka) |
|--------|---------|----------------------|---------------------------|
| Write target | Ingesters via token ring | Kafka partitions | Partition owners directly |
| Routing | Token → ingester | Token → partition → Kafka | Token → partition → owners |
| Failure isolation | Zone-level | Per-partition | Per-partition |
| Write durability | Ingester WAL | Kafka log | Ingester WAL |
| Read quorum | 2 of 3 zones (zone-aware) | 1 of N (Kafka guarantees) | 2 of 3 zones (zone-aware) |
| Ingester startup | Empty | Replay from Kafka | Empty (WAL replay only) |
| External dependency | None | Kafka | None |

---

## How It Works

### Partition Model

Each partition is identified by an integer ID. With 3 zones and N ingesters per zone:

```
Partition 0: owned by ingester-zone-a-0, ingester-zone-b-0, ingester-zone-c-0
Partition 1: owned by ingester-zone-a-1, ingester-zone-b-1, ingester-zone-c-1
...
Partition N-1: owned by ingester-zone-a-(N-1), ingester-zone-b-(N-1), ingester-zone-c-(N-1)
```

Each partition has:
- **Tokens**: Deterministic set of 512 tokens on the consistent hash ring
- **State**: PENDING, ACTIVE, INACTIVE, or DELETED
- **StateTimestamp**: When the state last changed
- **Owners**: List of ingesters (one per zone) that own this partition

**Key point**: Partition owners are also registered in the standard ingester ring. The distributor's ingester client pool includes partition owners since they share the same instance IDs.

**Edge case - owner in partition ring but not ingester ring**: If a partition owner is registered in the partition ring but missing from the ingester ring (e.g., ingester crashed and was removed from ingester ring first), that owner is treated as unavailable for that partition:
- **Writes**: Skip that owner, attempt to achieve quorum with remaining owners
- **Reads**: Skip that owner, count as one fewer healthy zone for that partition
- This is consistent with how Ingest Storage handles this case today

### Dynamic Partition Creation via Lifecycler

The existing `PartitionInstanceLifecycler` handles all partition lifecycle management. When an ingester starts:

1. **Compute partition ID** using the existing `IngesterPartitionID()` function:
   ```go
   // Extracts ordinal from instance name: ingester-zone-a-33 → 33
   partitionID, err := ingest.IngesterPartitionID(cfg.IngesterRing.InstanceID)
   ```

2. **Lifecycler creates partition and registers owner**:
   ```go
   i.ingestPartitionLifecycler = ring.NewPartitionInstanceLifecycler(
       i.cfg.IngesterPartitionRing.ToLifecyclerConfig(i.ingestPartitionID, cfg.IngesterRing.InstanceID),
       PartitionRingName,
       PartitionRingKey,
       partitionRingKV,
       logger,
       registerer,
   )
   ```

3. **Lifecycler handles PENDING → ACTIVE transition** automatically when:
   - Minimum number of owners registered (configurable, default: 3 for all zones)
   - Minimum duration elapsed (configurable, default: 10s)

4. **On ingester restart**: The lifecycler re-registers as owner. The partition ID is deterministic (based on instance name), so the same ingester always owns the same partition.

### Why 2-of-3 Quorum is Required (Without Kafka)

With Kafka, reads can use 1-of-N quorum because:
- All data is written to Kafka first (single source of truth)
- All partition owners consume the same Kafka log
- Any owner has complete data for the partition

Without Kafka, we write directly to partition owners:
- Writes go to all 3 owners, requiring 2-of-3 zones to ACK
- If write succeeds to zones A+B but not C, only A and B have the data
- Reads must query at least 2 zones to guarantee seeing all successfully written data

**Read quorum must match write quorum** to ensure consistency.

### Write Path

Writes are routed through the partition ring to partition owners. The key change from Kafka mode is that instead of writing to Kafka, we write directly to the partition's owner ingesters.

**Intent**: The existing `DoBatchWithOptions` callback receives a partition ID. In Kafka mode, this writes to Kafka. In no-Kafka mode, we need to:
1. Look up the partition's owners from the partition ring
2. Resolve those owner IDs to ingester instances via the ingester ring
3. Write to those ingesters with zone-aware quorum (2 of 3 zones must ACK)

**Sketch** (implementation details left to implementer):

```go
func (d *Distributor) sendWriteRequestToPartitions(ctx context.Context, ...) error {
    return ring.DoBatchWithOptions(ctx, ring.Write, tenantRing, keys,
        func(partition ring.InstanceDesc, indexes []int) error {
            partitionID := parsePartitionID(partition.Id)

            if d.cfg.IngestStorageConfig.Kafka.Enabled {
                // Existing path: write to Kafka
                return d.ingestStorageWriter.WriteSync(ctx, partitionID, tenantID, req)
            }

            // New path: write directly to partition owners
            return d.writeToPartitionOwners(ctx, partitionID, req)
        }, batchOptions)
}
```

**Pseudocode for `writeToPartitionOwners`**:

```go
func (d *Distributor) writeToPartitionOwners(ctx context.Context, partitionID int32, req *mimirpb.WriteRequest) error {
    // 1. Get partition owner IDs from partition ring
    ownerIDs := d.partitionsRing.PartitionRing().PartitionOwnerIDs(partitionID)

    // 2. Resolve owner IDs to InstanceDesc via ingester ring
    var instances []ring.InstanceDesc
    var healthyZones = make(map[string]bool)

    for _, ownerID := range ownerIDs {
        instance, err := d.ingestersRing.GetInstance(ownerID)
        if err != nil {
            continue // Owner not in ingester ring, skip
        }
        if !instance.IsHealthy(ring.Write, d.cfg.HeartbeatTimeout, time.Now()) {
            continue // Owner unhealthy, skip
        }
        instances = append(instances, instance)
        healthyZones[instance.Zone] = true
    }

    // 3. Validate we have enough zones for quorum BEFORE attempting write
    // Use EXPECTED zone count (typically 3), not healthy count
    expectedZones := d.cfg.ExpectedZoneCount // e.g., 3
    minRequiredZones := (expectedZones / 2) + 1 // e.g., 2 for 3 zones
    numHealthyZones := len(healthyZones)

    if numHealthyZones < minRequiredZones {
        return fmt.Errorf("partition %d: insufficient healthy zones for quorum (have %d, need %d of %d)",
            partitionID, numHealthyZones, minRequiredZones, expectedZones)
    }

    // 4. Build ReplicationSet with zone-aware quorum
    replicationSet := ring.ReplicationSet{
        Instances:            instances,
        ZoneAwarenessEnabled: true,
        MaxUnavailableZones:  numHealthyZones - minRequiredZones,
    }

    // 5. Write to owners with quorum
    // Use DoUntilQuorum or similar pattern from existing ingester write code
    _, err := ring.DoUntilQuorum(ctx, replicationSet,
        ring.DoUntilQuorumConfig{
            MinimizeRequests: false,
        },
        func(ctx context.Context, desc *ring.InstanceDesc) (any, error) {
            client, err := d.ingesterPool.GetClientForInstance(*desc)
            if err != nil {
                return nil, err
            }
            _, err = client.(ingester_client.IngesterClient).Push(ctx, req)
            return nil, err
        },
        func(any) {})

    return err
}
```

**Key requirements**:
- Validate quorum is achievable BEFORE attempting writes (step 3)
- Use EXPECTED zone count for `minRequiredZones`, not the healthy count
- Fail fast if insufficient healthy zones
- Use existing `DoUntilQuorum` or similar for zone-aware write execution

The implementer should follow existing patterns in the distributor for ingester writes, adapting them for the partition-based routing.

### Read Path

The read path returns one `ReplicationSet` per partition. The key difference from Kafka mode is the quorum requirement.

**Kafka mode (existing)**: Uses `MaxUnavailableZones: uniqueZones - 1`, meaning only 1 zone needs to respond. This is safe because Kafka guarantees all owners have the same data.

**No-Kafka mode (proposed)**: Uses `MaxUnavailableZones: 1` (with 3 zones), meaning 2 zones must respond. This matches the write quorum to ensure consistency.

**Intent**: Modify `GetReplicationSetsForOperation` (or add a variant) to use stricter quorum when Kafka is disabled. The existing code structure can be reused; only the `MaxUnavailableZones` calculation changes.

**Sketch**:

```go
// In partition_instance_ring.go, the key change is:
result = append(result, ReplicationSet{
    Instances:            instances,
    ZoneAwarenessEnabled: true,
    // Kafka mode: uniqueZones - 1 (need 1 zone)
    // No-Kafka mode: use quorum formula
    MaxUnavailableZones:  maxUnavailableZones, // Configurable based on mode
})
```

**Requirements**:
- Add configuration to `PartitionInstanceRing` to control quorum mode (Kafka vs no-Kafka). This will likely require a new config field or constructor parameter.
- For no-Kafka mode, use the quorum formula: `MaxUnavailableZones = uniqueZones - ((uniqueZones / 2) + 1)`
  - With 3 zones: `3 - (3/2 + 1) = 3 - 2 = 1` → need 2 zones
  - With 5 zones: `5 - (5/2 + 1) = 5 - 3 = 2` → need 3 zones
- Fail the query if fewer than `(uniqueZones / 2) + 1` zones have healthy owners for any partition
- This ensures reads see all data that was successfully written with quorum

### Failure Isolation

With per-partition evaluation:

```
Partition 5: zone-a-5 (healthy), zone-b-5 (unhealthy), zone-c-5 (healthy)
  → 2 of 3 zones healthy → partition 5 queries SUCCEED

Partition 7: zone-a-7 (healthy), zone-b-7 (healthy), zone-c-7 (healthy)
  → 3 of 3 zones healthy → partition 7 queries SUCCEED
```

Compare to classic zone contamination:
```
Zone-b has 1 unhealthy ingester (zone-b-5)
  → Entire zone-b excluded
  → If zone-a also has 1 unhealthy ingester → ALL queries FAIL
```

---

## Scaling Behavior

### Scale Up: Adding Ingesters

**Scenario**: Add `ingester-zone-a-33`, `ingester-zone-b-33`, `ingester-zone-c-33`

1. Each ingester starts and the lifecycler registers it as owner of partition 33
2. Partition 33 is created in PENDING state (if it doesn't exist)
3. Once all 3 zones have owners (and min duration elapsed), partition 33 → ACTIVE
4. Partition 33's tokens are added to the consistent hash ring
5. Series whose hashes land in partition 33's token range now route to it
6. Old data for those series is still on previous partitions
7. Queries go to ALL partitions, so old data is still found

**Partial deployment handling**: If ingesters don't start simultaneously across zones:
- Partition stays PENDING until all zones have owners
- Writes for that token range route to the next ACTIVE partition on the ring
- This causes temporary load imbalance but no data loss

### Scale Down: Removing Ingesters

**Scenario**: Remove `ingester-zone-a-33`, `ingester-zone-b-33`, `ingester-zone-c-33`

**Recommended procedure** (using rollout operator):

1. **Mark partition INACTIVE**: Call `POST /ingester/prepare-partition-downscale` on any ingester owning partition 33
   - This calls `ChangePartitionState(ctx, PartitionInactive)` on the lifecycler
   - Partition 33's tokens are effectively removed from write routing
   - Series that would route to partition 33 now route to next partition on ring

2. **Prepare for shutdown**: Call `POST /ingester/prepare-shutdown` on each ingester
   - Sets `RemoveOwnerOnShutdown(true)` on the partition lifecycler
   - Ingester stops accepting new writes
   - In-flight writes complete
   - Ingester flushes data to blocks

3. **Lookback period**: Queries still include partition 33 (INACTIVE within lookback via `ShuffleShardWithLookback`)

4. **Remove ingesters**: After lookback period (~2h), safe to terminate pods
   - On shutdown, each ingester removes itself as partition owner

5. **Cleanup**: Eventually partition 33 → DELETED (tombstone for gossip)
   - Other lifecyclers clean up partitions with no owners that have been INACTIVE long enough

### Shuffle Sharding

Tenants can be assigned a subset of partitions:

```go
// Tenant gets shardSize partitions via deterministic selection
subring := partitionsRing.ShuffleShard(tenantID, shardSize)

// For queries, include recently-removed partitions
subring := partitionsRing.ShuffleShardWithLookback(tenantID, shardSize, lookbackPeriod, now)
```

---

## Configuration

```yaml
ingest_storage:
  enabled: true

  # Disable Kafka, use direct ingester writes
  kafka:
    enabled: false

  # Migration controls
  migration:
    # Percentage of series to route via partition ring (0-100)
    # Increase gradually: 0 → 10 → 25 → 50 → 100
    write_percentage: 0

  # Enable per-partition query routing and failure isolation
  # Only set to true AFTER:
  # 1. write_percentage is 100
  # 2. Old classic-path data has aged out (~2h)
  # 3. You've validated partition health
  partition_isolation_enabled: false

  partition_ring:
    kvstore:
      store: memberlist  # or consul, etcd

    # Minimum owners before partition becomes ACTIVE
    # Set to 3 to require all zones
    min_partition_owners_count: 3

    # How long minimum owners must be registered
    min_partition_owners_duration: 10s

    # When to delete inactive partitions with no owners
    delete_inactive_partition_after: 13h

ingester:
  ring:
    instance_id: ingester-zone-a-0  # Ordinal determines partition ID
    zone: zone-a
```

### Configuration Validation

```go
func (cfg *IngestStorageConfig) Validate() error {
    if cfg.PartitionIsolationEnabled {
        if cfg.Migration.WritePercentage < 100 {
            return errors.New("cannot enable partition_isolation_enabled until write_percentage is 100")
        }
    }

    if cfg.Enabled && !cfg.Kafka.Enabled {
        if cfg.Kafka.Address != "" {
            return errors.New("kafka.address must be empty when kafka.enabled is false")
        }
    }

    return nil
}
```

### Rollout Operator Integration

The [Grafana Rollout Operator](https://github.com/grafana/rollout-operator) provides safe zone-by-zone rollouts:

```yaml
metadata:
  labels:
    rollout-group: ingester
  annotations:
    grafana.com/rollout-max-unavailable: "1"
    grafana.com/prepare-downscale: "true"
    grafana.com/prepare-downscale-http-path: "ingester/prepare-shutdown"
    grafana.com/prepare-downscale-http-port: "80"
```

The operator ensures:
- Pods in different StatefulSets (zones) are not rolled simultaneously
- Rollout only proceeds when all other zones are Ready
- `prepare-shutdown` endpoint called before pod termination

**For partition scale-down**, also configure the partition downscale endpoint:
```yaml
metadata:
  annotations:
    # First mark partition INACTIVE, then prepare shutdown
    grafana.com/prepare-downscale-http-path: "ingester/prepare-partition-downscale"
```

This marks the partition as INACTIVE before the ingester begins its shutdown sequence, ensuring writes are redirected to other partitions before the owner is removed.

---

## Migration Strategy

### Overview

The migration uses two independent controls:
- **`write_percentage`**: Controls what percentage of series use partition routing for writes (0-100)
- **`partition_isolation_enabled`**: Controls whether queries use per-partition isolation (explicit cutover)

**Key insight**: The token distributions are completely different between classic and partition routing. A series going to ingester-5 in classic mode might go to partition-17 in partition mode. This is why we need gradual migration with queries going to ALL ingesters until cutover.

### Migration Phases

| Phase | Config | Writes | Queries | Failure Mode |
|-------|--------|--------|---------|--------------|
| **1. Deploy** | `write_percentage: 0` | 100% classic | All ingesters | Zone contamination |
| **2. Gradual** | `write_percentage: 10→100` | Gradual shift | All ingesters | Zone contamination |
| **3. Complete** | `write_percentage: 100` | 100% partition | All ingesters | Zone contamination |
| **4. Aging** | Wait ~2h | 100% partition | All ingesters | Zone contamination |
| **5. Cutover** | `partition_isolation_enabled: true` | 100% partition | Per-partition | **Per-partition** |

### Phase Details

**Phase 1: Deploy partition ring (no traffic)**
```yaml
ingest_storage:
  enabled: true
  kafka:
    enabled: false
  migration:
    write_percentage: 0
  partition_isolation_enabled: false
```
- Ingesters register as partition owners via lifecycler
- Partitions are created and become ACTIVE
- No writes go to partition path yet
- Queries use classic zone-aware mode

**Phase 2-3: Gradual write migration**
```yaml
ingest_storage:
  migration:
    write_percentage: 10  # Then 25, 50, 75, 100
  partition_isolation_enabled: false
```
- Series hash determines path: `hash % 100 < write_percentage` → partition routing
- **This is NOT dual-write**: Each series goes to exactly ONE path based on its hash
- **Queries go to ALL ingesters** (because different series are on different paths)
- Zone contamination failure mode (stricter, safe during migration)

**Note on query fan-out**: Querying all ingesters during migration is not a regression if you already operate with a single large tenant (no shuffle sharding), as you're already querying all ingesters today.

**Phase 4: Wait for data aging**
```yaml
ingest_storage:
  migration:
    write_percentage: 100
  partition_isolation_enabled: false  # Not yet!
```
- All writes go to partition path
- **Queries still go to ALL ingesters** (old data may exist on classic path)
- Wait ~2h for old data to age out via head compaction
- Zone contamination still applies

**Phase 4→5 Validation**: Before enabling `partition_isolation_enabled`, verify:
```promql
# Verify no recent classic-path writes (should be 0 for at least 2h)
sum(rate(cortex_distributor_requests_in_total{path="classic"}[5m])) == 0

# Verify partition path is handling all traffic
sum(rate(cortex_distributor_requests_in_total{path="partition"}[5m])) > 0

# Verify oldest in-memory data is newer than migration time
# (Check ingester TSDB head block min time via /metrics endpoint)
```

**Phase 5: Explicit cutover**
```yaml
ingest_storage:
  migration:
    write_percentage: 100
  partition_isolation_enabled: true  # NOW enable per-partition isolation
```
- Queries now use per-partition routing
- Per-partition failure isolation is active
- Only safe because old data has aged out

### Implementation

```go
// Write path: percentage-based routing
func (d *Distributor) push(ctx context.Context, req *Request) error {
    // ... existing setup ...

    for _, series := range req.Timeseries {
        hash := tokenForLabels(userID, series.Labels)

        if d.usePartitionRouting(hash) {
            partitionKeys = append(partitionKeys, hash)
            partitionSeries = append(partitionSeries, series)
        } else {
            classicKeys = append(classicKeys, hash)
            classicSeries = append(classicSeries, series)
        }
    }

    // Send to both paths in parallel
    g, ctx := errgroup.WithContext(ctx)
    if len(classicSeries) > 0 {
        g.Go(func() error { return d.sendViaClassic(ctx, classicKeys, classicSeries) })
    }
    if len(partitionSeries) > 0 {
        g.Go(func() error { return d.sendViaPartition(ctx, partitionKeys, partitionSeries) })
    }
    return g.Wait()
}

func (d *Distributor) usePartitionRouting(hash uint32) bool {
    pct := d.cfg.Migration.WritePercentage
    if pct == 0 {
        return false
    }
    if pct >= 100 {
        return true
    }
    return (hash % 100) < uint32(pct)
}

// Read path: query both paths during migration, per-partition after cutover
func (d *Distributor) getIngesterReplicationSetsForQuery(ctx context.Context) ([]ring.ReplicationSet, error) {
    if d.cfg.PartitionIsolationEnabled {
        // Post-cutover: per-partition queries only
        return d.partitionsRing.GetReplicationSetsForOperation(readNoExtend)
    }

    // Pre-cutover: query ALL ingesters (classic ring)
    // This covers data on both classic and partition paths
    replicationSet, err := d.ingestersRing.GetReplicationSetForOperation(readNoExtend)
    if err != nil {
        return nil, err
    }
    return []ring.ReplicationSet{replicationSet}, nil
}
```

### Rollback

**Before cutover (Phase 1-4):**
```yaml
ingest_storage:
  migration:
    write_percentage: 0
```
Instant - writes revert to classic, queries already go to all ingesters.

**After cutover (Phase 5):**
```yaml
ingest_storage:
  partition_isolation_enabled: false
  migration:
    write_percentage: 0
```

**Rollback Safety Constraint**: Rollback is safe because of an architectural invariant:

> **Partition owners are the same instances as ingesters in the ingester ring.**
>
> - Partition 5 is owned by `ingester-zone-a-5`, `ingester-zone-b-5`, `ingester-zone-c-5`
> - These same instance IDs exist in the ingester ring
> - When we query "all ingesters" via the ingester ring, we include all partition owners
> - Therefore, data written to partition owners is found by classic-path queries

This invariant holds because:
1. Partition ID is derived from ingester ordinal (`IngesterPartitionID()`)
2. Ingesters register in BOTH the ingester ring AND the partition ring with the same instance ID
3. There is no separate "partition owner" process - ingesters ARE partition owners

**After rollback**: Queries go to all ingesters (which includes all partition owners), so all data is accessible. Wait for the maximum query lookback window (typically 12-13h) before considering the rollback complete, to ensure any in-flight queries have completed.

### Migration Validation Checklist

**Phase 1→2 Transition:**
- [ ] All ingesters appear in `/partition-ring` endpoint
- [ ] All partitions are in ACTIVE state
- [ ] `cortex_partition_healthy_owners` shows 3 for all partitions
- [ ] Set `write_percentage: 10`
- [ ] Verify `cortex_distributor_write_requests_total{path="partition"}` is ~10% of total

**Phase 4→5 Transition:**
- [ ] `write_percentage: 100` for at least 2 hours
- [ ] Classic-path data has aged out (check ingester head block timestamps)
- [ ] Run validation query comparing classic vs partition paths
- [ ] Set `partition_isolation_enabled: true`
- [ ] Monitor partition health and query latency

---

## Operational Considerations

### Monitoring

```go
var (
    // Write path metrics
    writePathRequests = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "cortex_distributor_write_requests_total",
        Help: "Total write requests by path",
    }, []string{"path"}) // path = "classic" | "partition"

    partitionWriteLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "cortex_distributor_partition_write_latency_seconds",
        Help:    "Latency of partition writes",
        Buckets: prometheus.DefBuckets,
    }, []string{"partition_id"})

    partitionWriteZoneSuccess = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "cortex_distributor_partition_write_zone_success_total",
        Help: "Successful partition writes by zone",
    }, []string{"partition_id", "zone"})

    // Partition health metrics
    partitionHealthyOwners = promauto.NewGaugeVec(prometheus.GaugeOpts{
        Name: "cortex_partition_healthy_owners",
        Help: "Number of healthy owners per partition",
    }, []string{"partition_id"})

    partitionState = promauto.NewGaugeVec(prometheus.GaugeOpts{
        Name: "cortex_partition_state",
        Help: "Partition state (1=pending, 2=active, 3=inactive)",
    }, []string{"partition_id"})

    // Migration metrics
    migrationWritePercentage = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "cortex_distributor_migration_write_percentage",
        Help: "Current migration write percentage setting",
    })
)
```

### Alerting

```yaml
groups:
  - name: partition-ring
    rules:
      # Alert when partition loses fault tolerance (can't tolerate another failure)
      - alert: PartitionDegraded
        expr: cortex_partition_healthy_owners < 3
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Partition {{ $labels.partition_id }} has only {{ $value }} healthy owners"

      # Alert when partition has lost quorum
      - alert: PartitionCritical
        expr: cortex_partition_healthy_owners < 2
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Partition {{ $labels.partition_id }} has lost quorum ({{ $value }} healthy owners)"

      # Alert when partition has no healthy owners
      - alert: PartitionUnavailable
        expr: cortex_partition_healthy_owners == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Partition {{ $labels.partition_id }} has no healthy owners"

      # Alert on stuck PENDING partitions
      - alert: PartitionPendingTooLong
        expr: cortex_partition_state == 1 and changes(cortex_partition_state[10m]) == 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Partition {{ $labels.partition_id }} stuck in PENDING state"
```

### Debugging

Ring status endpoint shows partition state:
```
GET /partition-ring

Partition 0: ACTIVE (since 2024-01-01T00:00:00Z)
  Owners: ingester-zone-a-0 (healthy), ingester-zone-b-0 (healthy), ingester-zone-c-0 (healthy)
  Tokens: 512

Partition 1: ACTIVE (since 2024-01-01T00:00:00Z)
  Owners: ingester-zone-a-1 (healthy), ingester-zone-b-1 (unhealthy), ingester-zone-c-1 (healthy)
  Tokens: 512
  WARNING: Degraded - only 2 healthy zones
```

### Troubleshooting

**Partition stuck in PENDING:**
- Check if all 3 zones have ingesters for that partition ID
- Verify `min_partition_owners_count` and `min_partition_owners_duration` settings
- Check lifecycler logs for registration errors

**Write failures after cutover:**
- Check `cortex_partition_healthy_owners` for affected partition
- Verify ingester health in both partition ring and ingester ring
- Check for network issues between distributor and ingesters

**Query inconsistencies during migration:**
- Ensure `partition_isolation_enabled: false` during migration
- Verify all ingesters are being queried (not just partition owners)
- Check for data on both classic and partition paths

---

## Trade-offs

### What We Gain

1. **Per-partition failure isolation** - No zone contamination
2. **Same failure tolerance as Ingest Storage** - ~100% availability at 20 random failures vs 0% for classic
3. **No external dependencies** - No Kafka to operate
4. **Reuse battle-tested code** - Partition ring is production-proven
5. **Gradual migration** - Percentage-based rollout with explicit cutover

### What We Lose (vs Ingest Storage with Kafka)

1. **No write durability before ingester** - Writes fail if ingesters unavailable (same as classic)
2. **No cross-zone catch-up** - Ingesters replay their own WAL after restart, but can't catch up on data written to other zones while they were down (Kafka allows this)
3. **2 of 3 quorum required** - Can't use 1-of-N reads since there's no Kafka to guarantee all owners have complete data

### WAL Behavior

Ingester WAL behavior is **unchanged** from classic mode:
- Each ingester maintains its own WAL
- On restart, ingester replays its WAL to recover data it had before crash
- WAL does NOT help recover data that was written to other zones during downtime
- This is the same durability model as classic Mimir (and differs from Kafka mode where ingesters replay from Kafka)

### What We Lose (vs Classic)

1. **Slightly more complex routing** - Partition lookup vs direct token lookup
2. **New ring to operate** - Partition ring in addition to ingester ring

---

## Implementation Checklist

All items below are **new code to be added**. The design reuses existing infrastructure (partition ring, lifecycler, etc.) but requires new code for the no-Kafka write path and migration logic.

### dskit/ring

| File | Change |
|------|--------|
| `partition_instance_ring.go` | Add config option to control quorum mode (1-of-N vs 2-of-3) |

### mimir/pkg/distributor

| File | Change |
|------|--------|
| `distributor.go` | Add `writeToPartitionOwners()` function for direct ingester writes |
| `distributor.go` | Add conditional routing in `sendWriteRequestToPartitions()` based on `kafka.enabled` |
| `distributor.go` | Add percentage-based write routing for migration (`write_percentage`) |
| `query.go` | Add query path selection based on `partition_isolation_enabled` |
| `config.go` | Add new config fields: `Kafka.Enabled`, `Migration.WritePercentage`, `PartitionIsolationEnabled` |

### mimir/pkg/ingester

| File | Change |
|------|--------|
| `ingester.go` | Conditionally skip Kafka reader setup when `kafka.enabled: false` |
| `ingester.go` | Ensure partition lifecycler still runs (already does, just verify) |

### mimir/pkg/storage/ingest

| File | Change |
|------|--------|
| `config.go` | Add `Kafka.Enabled bool` field to `KafkaConfig` struct |
| `config.go` | Add validation: if `Enabled=true` and `Kafka.Enabled=false`, certain Kafka fields should be empty |

---

## Summary

This design provides per-partition failure isolation by reusing the existing partition ring infrastructure without Kafka:

1. **Reuse partition ring** - State machine, ownership, tokens, shuffle sharding (all existing)
2. **Reuse partition lifecycler** - Handles partition creation, owner registration, state transitions
3. **Direct ingester writes** - Skip Kafka, write to partition owners via existing ingester client pool
4. **Zone-aware quorum** - Require 2 of 3 zones for reads and writes (since no Kafka to guarantee consistency)
5. **Gradual migration** - Percentage-based write routing, explicit cutover for query path
6. **Operational safety** - Rollout operator integration, comprehensive monitoring

The implementation is minimal because we're reusing the partition ring infrastructure that's already battle-tested in Ingest Storage.

---

## Test Plan

### Tests to Duplicate/Adapt from Ingest Storage

**Distributor Write Path** (from `distributor_ingest_storage_test.go`):
- `TestDistributor_Push_ShouldSupportIngestStorage` → Adapt for direct ingester writes
- `TestDistributor_Push_ShouldSupportWriteBothToIngestersAndPartitions` → Migration dual-path test
- `TestDistributor_Push_ShouldCleanupWriteRequestAfterWritingBothToIngestersAndPartitions`
- `TestDistributor_Push_IgnoreIngestStorageErrorsDuringMigration`

**Distributor Query Path** (from `query_ingest_storage_test.go`):
- `TestDistributor_QueryStream_ShouldSupportIngestStorage` → Adapt for 2-of-3 quorum

**Partition Ring** (from `partition_instance_ring_test.go`):
- `TestPartitionInstanceRing_GetReplicationSetsForOperation` → Adapt for strict quorum mode

### New Tests: Write Path Correctness

| Test | Purpose |
|------|---------|
| `TestDistributor_WriteToPartitionOwners_Quorum` | Verify 2-of-3 zone quorum: 3 healthy → success, 2 healthy → success, 1 healthy → fail |
| `TestDistributor_WriteToPartitionOwners_OwnerNotInIngesterRing` | Verify graceful handling when partition owner missing from ingester ring |
| `TestDistributor_WriteToPartitionOwners_QuorumValidationBeforeWrite` | Verify fail-fast when quorum not achievable |
| `TestDistributor_WriteToPartitionOwners_PartialFailure` | Verify correct behavior when some owners fail during write |

### New Tests: Read Path Correctness

| Test | Purpose |
|------|---------|
| `TestPartitionInstanceRing_GetReplicationSetsForOperation_StrictQuorum` | Verify MaxUnavailableZones uses quorum formula |
| `TestDistributor_Query_ReadsFromAllPartitions` | Verify queries go to ALL partitions, not per-series routing |

### New Tests: Migration

| Test | Purpose |
|------|---------|
| `TestDistributor_Migration_WritePercentageRouting` | Verify series routed based on hash % 100 < write_percentage |
| `TestDistributor_Migration_QueryAllIngestersDuringMigration` | Verify queries hit all ingesters when partition_isolation_enabled=false |
| `TestDistributor_Migration_CutoverValidation` | Verify cutover blocked if write_percentage < 100 |
| `TestDistributor_Migration_Rollback` | Verify data accessible after rollback |

### New Tests: Scale Up/Down

| Test | Purpose |
|------|---------|
| `TestPartition_ScaleUp_NewPartitionBehavior` | Verify new writes go to new partition, old data still found |
| `TestPartition_ScaleDown_InactivePartitionLookback` | Verify INACTIVE partitions queried during lookback |
| `TestPartition_ScaleDown_PreparePartitionDownscale` | Verify endpoint marks partition INACTIVE |

### New Tests: Failure Scenarios

| Test | Purpose |
|------|---------|
| `TestPartition_IngesterCrash_QuorumMaintained` | Verify 1-of-3 crash doesn't break writes/reads |
| `TestPartition_IngesterCrash_QuorumLost` | Verify 2-of-3 crash fails with clear error |
| `TestPartition_IngesterRestart_RejoinsPartition` | Verify restarted ingester re-registers as owner |
| `TestPartition_NetworkPartition_ZoneIsolation` | Verify zone isolation handled correctly |

### New Tests: Dynamic Zone Count

| Test | Purpose |
|------|---------|
| `TestPartition_Quorum_TwoZones` | Verify 2-zone behavior (no fault tolerance) |
| `TestPartition_Quorum_FiveZones` | Verify 5-zone quorum math (need 3 of 5) |

### New Tests: Configuration

| Test | Purpose |
|------|---------|
| `TestConfig_Validation` | Verify all invalid config combinations rejected |

### Integration Tests

| Test | Purpose |
|------|---------|
| `TestE2E_WriteAndQuery_NoKafka` | Full end-to-end without Kafka |
| `TestE2E_PartitionFailure_Recovery` | Recovery from partition owner failure |
| `TestE2E_Migration_ClassicToPartition` | Full migration path |
| `TestE2E_Migration_Rollback` | Full rollback scenario |

### Test Priority

**Critical (block release)**:
1. `TestDistributor_WriteToPartitionOwners_Quorum`
2. `TestPartitionInstanceRing_GetReplicationSetsForOperation_StrictQuorum`
3. `TestDistributor_Query_ReadsFromAllPartitions`
4. `TestDistributor_Migration_QueryAllIngestersDuringMigration`
5. `TestPartition_IngesterCrash_QuorumMaintained`

**High Priority**:
6. `TestDistributor_WriteToPartitionOwners_QuorumValidationBeforeWrite`
7. `TestDistributor_Migration_WritePercentageRouting`
8. `TestPartition_ScaleDown_InactivePartitionLookback`
9. `TestConfig_Validation`
10. `TestE2E_WriteAndQuery_NoKafka`

---

*Design document created 2025-12-06*
*Last updated: 2025-12-06 - Fixed quorum validation, added phase 4→5 validation, documented rollback safety*
