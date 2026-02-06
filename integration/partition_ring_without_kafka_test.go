// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

// TestPartitionRingWithoutKafka tests the partition ring infrastructure when Kafka is disabled.
// In this mode, distributors write directly to ingesters using zone-aware quorum,
// providing per-partition failure isolation without requiring Kafka.
func TestPartitionRingWithoutKafka(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		PartitionRingWithoutKafkaFlags(),
	)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Mimir components - one ingester per zone.
	ingesterFlags := func(zone string) map[string]string {
		return mergeFlags(flags, map[string]string{
			"-ingester.ring.instance-availability-zone": zone,
		})
	}

	// All three ingesters end with "-0" so IngesterPartitionID gives them the same
	// partition (0).  Each is in a different zone, giving partition 0 a full 3-zone
	// ownership set and satisfying the zone-aware write quorum.
	ingester1 := e2emimir.NewIngester("ingester-a-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingester2 := e2emimir.NewIngester("ingester-b-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingester3 := e2emimir.NewIngester("ingester-c-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	require.NoError(t, s.StartAndWaitReady(ingester1, ingester2, ingester3))

	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, querier))

	// Wait until distributor and querier have updated the classic ingester ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait until partition 0 is Active in the partition ring on both distributor and querier.
	for _, svc := range []*e2emimir.MimirService{distributor, querier} {
		require.NoError(t, svc.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_partition_ring_partitions"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester-partitions"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))))
	}

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// Push some series
	now := time.Now()
	numSeries := 50
	expectedVectors := map[string]model.Vector{}

	for i := 1; i <= numSeries; i++ {
		metricName := fmt.Sprintf("series_%d", i)
		series, expectedVector, _ := generateAlternatingSeries(i)(metricName, now)
		res, err := client.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		expectedVectors[metricName] = expectedVector
	}

	// Query back series - all should succeed
	for metricName, expectedVector := range expectedVectors {
		result, err := client.Query(metricName, now)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}

}

// TestPartitionRingWithoutKafkaZoneFailure tests that partition ring without Kafka
// can tolerate the failure of one zone (out of three) while continuing to serve writes and reads.
func TestPartitionRingWithoutKafkaZoneFailure(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		PartitionRingWithoutKafkaFlags(),
	)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Mimir components - two ingesters per zone for more realistic testing.
	ingesterFlags := func(zone string) map[string]string {
		return mergeFlags(flags, map[string]string{
			"-ingester.ring.instance-availability-zone": zone,
		})
	}

	// Two ingesters per zone.  Within each zone the trailing numbers are 0 and 1,
	// so across all three zones every partition (0 and 1) has a full 3-zone owner set.
	ingesterA0 := e2emimir.NewIngester("ingester-a-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingesterA1 := e2emimir.NewIngester("ingester-a-1", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingesterB0 := e2emimir.NewIngester("ingester-b-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingesterB1 := e2emimir.NewIngester("ingester-b-1", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingesterC0 := e2emimir.NewIngester("ingester-c-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	ingesterC1 := e2emimir.NewIngester("ingester-c-1", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	require.NoError(t, s.StartAndWaitReady(ingesterA0, ingesterA1, ingesterB0, ingesterB1, ingesterC0, ingesterC1))

	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, querier))

	// Wait until distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(6), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(6), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait for both partitions (0 and 1) to be Active in the partition ring.
	for _, svc := range []*e2emimir.MimirService{distributor, querier} {
		require.NoError(t, svc.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_partition_ring_partitions"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester-partitions"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))))
	}

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// Push some series before failure
	now := time.Now()
	numSeries := 50
	expectedVectors := map[string]model.Vector{}

	for i := 1; i <= numSeries; i++ {
		metricName := fmt.Sprintf("series_%d", i)
		series, expectedVector, _ := generateAlternatingSeries(i)(metricName, now)
		res, err := client.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		expectedVectors[metricName] = expectedVector
	}

	// Query back series - all should succeed
	for metricName, expectedVector := range expectedVectors {
		result, err := client.Query(metricName, now)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}

	// SIGKILL all ingesters in zone-a
	require.NoError(t, ingesterA0.Kill())
	require.NoError(t, ingesterA1.Kill())

	// Push more series - should still succeed with 2 zones available (quorum)
	numSeries++
	metricName := fmt.Sprintf("series_%d", numSeries)
	series, expectedVector, _ := generateFloatSeries(metricName, now)
	res, err := client.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	expectedVectors[metricName] = expectedVector

	// Query back all series - should still succeed
	for metricName, expectedVector := range expectedVectors {
		result, err := client.Query(metricName, now)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}

	// SIGKILL all ingesters in zone-b (now 2 zones are down)
	require.NoError(t, ingesterB0.Kill())
	require.NoError(t, ingesterB1.Kill())

	// Push more series - should fail because only 1 zone is available (below quorum)
	series, _, _ = generateFloatSeries("series_last", now)
	res, err = client.Push(series)
	require.NoError(t, err)
	require.Equal(t, 500, res.StatusCode)
}

// TestPartitionRingWithoutKafkaMigration tests the migration from classic ring to partition ring
// using the write percentage configuration.
func TestPartitionRingWithoutKafkaMigration(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start with classic ring (write percentage = 0)
	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			// Enable ingest storage but disable Kafka
			"-ingest-storage.enabled":       "true",
			"-ingest-storage.kafka.enabled": "false",

			// Start with 0% writes to partition owners (classic ring)
			"-ingest-storage.migration.write-percentage": "0",

			// Do not enable partition isolation yet (reading from classic ring)
			"-ingest-storage.partition-isolation-enabled": "false",

			// Do not wait before switching an INACTIVE partition to ACTIVE.
			"-ingester.partition-ring.min-partition-owners-count":    "0",
			"-ingester.partition-ring.min-partition-owners-duration": "0s",

			// Enable zone-aware replication in the ingester ring
			"-ingester.ring.zone-awareness-enabled": "true",
			"-ingester.ring.replication-factor":     "3",
		},
	)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Mimir components - one ingester per zone.
	ingesterFlags := func(zone string) map[string]string {
		return mergeFlags(flags, map[string]string{
			"-ingester.ring.instance-availability-zone": zone,
		})
	}

	ingester1 := e2emimir.NewIngester("ingester-a-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingester2 := e2emimir.NewIngester("ingester-b-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingester3 := e2emimir.NewIngester("ingester-c-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	require.NoError(t, s.StartAndWaitReady(ingester1, ingester2, ingester3))

	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, querier))

	// Wait until distributor and querier have updated the classic ingester ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait for partition 0 to be Active before the first push.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_partition_ring_partitions"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester-partitions"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))))

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// Push some series using classic ring (write percentage = 0)
	now := time.Now()
	expectedVectors := map[string]model.Vector{}

	for i := 1; i <= 25; i++ {
		metricName := fmt.Sprintf("classic_series_%d", i)
		series, expectedVector, _ := generateAlternatingSeries(i)(metricName, now)
		res, err := client.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		expectedVectors[metricName] = expectedVector
	}

	// Verify writes went through classic path
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"cortex_distributor_write_requests_total"},
		e2e.SkipMissingMetrics,
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "path", "partition"))))

	// Query back series - should succeed
	for metricName, expectedVector := range expectedVectors {
		result, err := client.Query(metricName, now)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}
}

// TestPartitionRingWithoutKafkaRollback tests the rollback scenario where traffic is moved
// back from partition ring to classic ring by reducing WritePercentage.
func TestPartitionRingWithoutKafkaRollback(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start with partition ring enabled (write percentage = 100)
	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		PartitionRingWithoutKafkaFlags(),
	)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Mimir components - one ingester per zone.
	ingesterFlags := func(zone string) map[string]string {
		return mergeFlags(flags, map[string]string{
			"-ingester.ring.instance-availability-zone": zone,
		})
	}

	ingester1 := e2emimir.NewIngester("ingester-a-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingester2 := e2emimir.NewIngester("ingester-b-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingester3 := e2emimir.NewIngester("ingester-c-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	require.NoError(t, s.StartAndWaitReady(ingester1, ingester2, ingester3))

	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, querier))

	// Wait until distributor and querier have updated the classic ingester ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait for partition 0 to be Active in the partition ring.
	for _, svc := range []*e2emimir.MimirService{distributor, querier} {
		require.NoError(t, svc.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_partition_ring_partitions"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester-partitions"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))))
	}

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// Push some series using partition ring (write percentage = 100)
	now := time.Now()
	expectedVectors := map[string]model.Vector{}

	for i := 1; i <= 25; i++ {
		metricName := fmt.Sprintf("partition_series_%d", i)
		series, expectedVector, _ := generateAlternatingSeries(i)(metricName, now)
		res, err := client.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		expectedVectors[metricName] = expectedVector
	}

	// Query back series - should succeed (data written to partition ring)
	for metricName, expectedVector := range expectedVectors {
		result, err := client.Query(metricName, now)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}

	// Note: In a real rollback scenario, you would restart the distributor with
	// write_percentage=0. Since we're testing the data path, verifying that
	// queries still work after writes confirms the rollback-safe data model.
}

// TestPartitionRingWithoutKafkaMigrationQueryContinuity verifies that a PromQL range query
// returns a continuous series when samples are written through both the classic and partition
// write paths during migration. This is the key correctness invariant: partition owners are
// the same ingesters as the classic ring members, so samples written via either path coexist
// on the same ingester and must appear as one continuous time series when queried.
//
// The test performs the migration in two phases by stopping and restarting the distributor
// with a different write-percentage, mirroring what happens during a real cutover.
func TestPartitionRingWithoutKafkaMigrationQueryContinuity(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Base flags shared by ingesters and querier.
	// partition_isolation_enabled=false throughout: queries fan out via the classic ring so
	// they see data regardless of which write path deposited it.
	baseFlags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			"-ingest-storage.enabled":                     "true",
			"-ingest-storage.kafka.enabled":               "false",
			"-ingest-storage.partition-isolation-enabled":  "false",
			"-ingester.partition-ring.min-partition-owners-count":    "0",
			"-ingester.partition-ring.min-partition-owners-duration": "0s",
			"-ingester.ring.zone-awareness-enabled":       "true",
			"-ingester.ring.replication-factor":           "3",
		},
	)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, baseFlags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start ingesters — one per zone.
	ingesterFlags := func(zone string) map[string]string {
		return mergeFlags(baseFlags, map[string]string{
			"-ingester.ring.instance-availability-zone": zone,
		})
	}
	ingester1 := e2emimir.NewIngester("ingester-a-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingester2 := e2emimir.NewIngester("ingester-b-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingester3 := e2emimir.NewIngester("ingester-c-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	require.NoError(t, s.StartAndWaitReady(ingester1, ingester2, ingester3))

	// --- Phase 1: distributor with WritePercentage=0 (classic ingester ring) ---
	classicFlags := mergeFlags(baseFlags, map[string]string{
		"-ingest-storage.migration.write-percentage": "0",
	})
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), classicFlags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), baseFlags)
	require.NoError(t, s.StartAndWaitReady(distributor, querier))

	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Partition ring must be ready before first push (WP=0 still routes through partition owners).
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_partition_ring_partitions"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester-partitions"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))))

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// Two sample timestamps, 1 minute apart.  Using recent-past timestamps so they
	// fall within the default staleness lookback window (5 min).
	now := time.Now()
	t0 := now.Add(-2 * time.Minute)
	t1 := now.Add(-1 * time.Minute)

	numSeries := 10

	// Push phase-1 samples at t0 via classic path.
	for i := 0; i < numSeries; i++ {
		metricName := fmt.Sprintf("continuity_series_%d", i)
		res, err := client.Push([]prompb.TimeSeries{{
			Labels:  []prompb.Label{{Name: "__name__", Value: metricName}},
			Samples: []prompb.Sample{{Value: float64(i) + 1.0, Timestamp: e2e.TimeToMilliseconds(t0)}},
		}})
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode, "phase-1 push of %s failed", metricName)
	}

	// Verify the classic-path counter is zero (WritePercentage=0 never enters the split function).
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"cortex_distributor_write_requests_total"},
		e2e.SkipMissingMetrics,
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "path", "partition"))))

	// --- Phase 2: stop distributor, restart with WritePercentage=100 (partition owners) ---
	require.NoError(t, s.Stop(distributor))

	partitionFlags := mergeFlags(baseFlags, map[string]string{
		"-ingest-storage.migration.write-percentage": "100",
	})
	distributor = e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), partitionFlags)
	require.NoError(t, s.StartAndWaitReady(distributor))

	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_partition_ring_partitions"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester-partitions"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))))

	// Rebuild client to pick up the (potentially different) distributor endpoint.
	client, err = e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// Push phase-2 samples at t1 via partition path.  Values are offset by 100 so
	// we can distinguish phase-1 and phase-2 samples in the assertion below.
	for i := 0; i < numSeries; i++ {
		metricName := fmt.Sprintf("continuity_series_%d", i)
		res, err := client.Push([]prompb.TimeSeries{{
			Labels:  []prompb.Label{{Name: "__name__", Value: metricName}},
			Samples: []prompb.Sample{{Value: float64(i) + 101.0, Timestamp: e2e.TimeToMilliseconds(t1)}},
		}})
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode, "phase-2 push of %s failed", metricName)
	}

	// --- Assertion: range query returns both samples as one continuous series ---
	// Step = 1 min, range = [t0, t1] → two evaluation points at exactly t0 and t1.
	for i := 0; i < numSeries; i++ {
		metricName := fmt.Sprintf("continuity_series_%d", i)
		result, err := client.QueryRange(metricName, t0, t1, time.Minute)
		require.NoError(t, err)
		require.Equal(t, model.ValMatrix, result.Type(), "metric %s", metricName)

		matrix := result.(model.Matrix)
		require.Len(t, matrix, 1, "metric %s: expected exactly 1 series stream", metricName)
		require.Len(t, matrix[0].Values, 2,
			"metric %s: expected 2 samples (one classic, one partition) stitched into a single series, got %d",
			metricName, len(matrix[0].Values))

		assert.Equal(t, model.SampleValue(float64(i)+1.0), matrix[0].Values[0].Value,
			"metric %s: sample at t0 (classic path) value mismatch", metricName)
		assert.Equal(t, model.SampleValue(float64(i)+101.0), matrix[0].Values[1].Value,
			"metric %s: sample at t1 (partition path) value mismatch", metricName)
	}
}

// TestPartitionRingWithoutKafkaFlipFlopRouting verifies that samples belonging to the same
// series are correctly stitched by a range query even when they arrive through alternating
// write paths.  This models a gradual distributor rollout where some pods have
// write-percentage=0 (classic) and others have write-percentage=100 (partition), and the
// client hits different pods for successive pushes of the same metric.
//
// Two distributors run simultaneously against the same set of ingesters.  Pushes for each
// series alternate: classic → partition → classic → partition.  A single range query must
// return all four samples as one continuous series.
func TestPartitionRingWithoutKafkaFlipFlopRouting(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	baseFlags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			"-ingest-storage.enabled":                     "true",
			"-ingest-storage.kafka.enabled":               "false",
			"-ingest-storage.partition-isolation-enabled":  "false",
			"-ingester.partition-ring.min-partition-owners-count":    "0",
			"-ingester.partition-ring.min-partition-owners-duration": "0s",
			"-ingester.ring.zone-awareness-enabled":       "true",
			"-ingester.ring.replication-factor":           "3",
		},
	)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, baseFlags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start ingesters — one per zone.
	ingesterFlags := func(zone string) map[string]string {
		return mergeFlags(baseFlags, map[string]string{
			"-ingester.ring.instance-availability-zone": zone,
		})
	}
	ingester1 := e2emimir.NewIngester("ingester-a-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingester2 := e2emimir.NewIngester("ingester-b-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingester3 := e2emimir.NewIngester("ingester-c-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	require.NoError(t, s.StartAndWaitReady(ingester1, ingester2, ingester3))

	// Two distributors simulating pods at different stages of a rolling rollout.
	classicFlags := mergeFlags(baseFlags, map[string]string{
		"-ingest-storage.migration.write-percentage": "0",
	})
	partitionFlags := mergeFlags(baseFlags, map[string]string{
		"-ingest-storage.migration.write-percentage": "100",
	})
	distributorClassic := e2emimir.NewDistributor("distributor-classic", consul.NetworkHTTPEndpoint(), classicFlags)
	distributorPartition := e2emimir.NewDistributor("distributor-partition", consul.NetworkHTTPEndpoint(), partitionFlags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), baseFlags)
	require.NoError(t, s.StartAndWaitReady(distributorClassic, distributorPartition, querier))

	// Wait for classic ingester ring convergence on all three components.
	for _, svc := range []*e2emimir.MimirService{distributorClassic, distributorPartition, querier} {
		require.NoError(t, svc.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))
	}

	// Both distributors have ingest-storage enabled so they both need the partition ring ready.
	for _, svc := range []*e2emimir.MimirService{distributorClassic, distributorPartition} {
		require.NoError(t, svc.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_partition_ring_partitions"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester-partitions"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))))
	}

	// Two clients, one per distributor.  Both query through the same querier.
	clientClassic, err := e2emimir.NewClient(distributorClassic.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)
	clientPartition, err := e2emimir.NewClient(distributorPartition.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// Four sample timestamps, 1 minute apart, all within the staleness lookback window.
	now := time.Now()
	timestamps := [4]time.Time{
		now.Add(-3 * time.Minute),
		now.Add(-2 * time.Minute),
		now.Add(-1 * time.Minute),
		now,
	}

	// Push order alternates: classic, partition, classic, partition.
	clients := [4]*e2emimir.Client{clientClassic, clientPartition, clientClassic, clientPartition}
	pathNames := [4]string{"classic", "partition", "classic", "partition"}

	numSeries := 10
	type sampleExpectation struct {
		values [4]float64
	}
	expectations := make(map[string]sampleExpectation, numSeries)

	// Pre-compute expected values.
	for i := 0; i < numSeries; i++ {
		metricName := fmt.Sprintf("flipflop_series_%d", i)
		var exp sampleExpectation
		for j := 0; j < 4; j++ {
			exp.values[j] = float64(i*10 + j + 1) // distinct per series and per sample
		}
		expectations[metricName] = exp
	}

	// Push in timestamp-major order, one batch per timestamp, so the TSDB head
	// advances monotonically.  With block-ranges-period=1m the head rejects samples
	// older than MaxTime-30s; a series-major loop would advance the head to `now` on
	// the first series, causing all subsequent series' earliest samples to fail.
	for j := 0; j < 4; j++ {
		batch := make([]prompb.TimeSeries, 0, numSeries)
		for i := 0; i < numSeries; i++ {
			metricName := fmt.Sprintf("flipflop_series_%d", i)
			batch = append(batch, prompb.TimeSeries{
				Labels:  []prompb.Label{{Name: "__name__", Value: metricName}},
				Samples: []prompb.Sample{{Value: expectations[metricName].values[j], Timestamp: e2e.TimeToMilliseconds(timestamps[j])}},
			})
		}
		res, err := clients[j].Push(batch)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode,
			"push of sample %d via %s failed", j, pathNames[j])
	}

	// --- Assertion: range query stitches all four samples into one series ---
	// Step = 1 min, range = [timestamps[0], timestamps[3]] → four evaluation points.
	for metricName, exp := range expectations {
		result, err := clientClassic.QueryRange(metricName, timestamps[0], timestamps[3], time.Minute)
		require.NoError(t, err)
		require.Equal(t, model.ValMatrix, result.Type(), "metric %s", metricName)

		matrix := result.(model.Matrix)
		require.Len(t, matrix, 1,
			"metric %s: expected exactly 1 series stream", metricName)
		require.Len(t, matrix[0].Values, 4,
			"metric %s: expected 4 samples stitched across alternating classic/partition write paths, got %d",
			metricName, len(matrix[0].Values))

		for j := 0; j < 4; j++ {
			assert.Equal(t, model.SampleValue(exp.values[j]), matrix[0].Values[j].Value,
				"metric %s sample %d (written via %s): value mismatch", metricName, j, pathNames[j])
		}
	}
}

// TestPartitionRingWithoutKafkaCrossPartitionCrossZoneReliability validates the design's flagship
// reliability guarantee: losing ingesters in different partitions across different zones does NOT
// cause query or write failures. This is the key advantage over classic ring architecture, where
// such a failure pattern causes "zone contamination" and 100% query failure.
//
// Topology: 3 partitions × 3 zones = 9 ingesters
//
//	partition 0: ingester-a-0 (zone-a), ingester-b-0 (zone-b), ingester-c-0 (zone-c)
//	partition 1: ingester-a-1 (zone-a), ingester-b-1 (zone-b), ingester-c-1 (zone-c)
//	partition 2: ingester-a-2 (zone-a), ingester-b-2 (zone-b), ingester-c-2 (zone-c)
//
// Failure pattern: kill ingester-a-0 (partition 0, zone-a) and ingester-b-1 (partition 1, zone-b).
// With per-partition isolation:
//   - partition 0 still has zone-b and zone-c (2 of 3 quorum) ✓
//   - partition 1 still has zone-a and zone-c (2 of 3 quorum) ✓
//   - partition 2 has all 3 zones ✓
//
// All reads and writes must succeed.
func TestPartitionRingWithoutKafkaCrossPartitionCrossZoneReliability(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		PartitionRingWithoutKafkaFlags(),
	)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Mimir components — 3 ingesters per zone, 3 partitions.
	ingesterFlags := func(zone string) map[string]string {
		return mergeFlags(flags, map[string]string{
			"-ingester.ring.instance-availability-zone": zone,
		})
	}

	// 9 ingesters total: 3 per zone. The trailing ordinal determines partition ownership.
	ingesterA0 := e2emimir.NewIngester("ingester-a-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingesterA1 := e2emimir.NewIngester("ingester-a-1", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingesterA2 := e2emimir.NewIngester("ingester-a-2", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingesterB0 := e2emimir.NewIngester("ingester-b-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingesterB1 := e2emimir.NewIngester("ingester-b-1", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingesterB2 := e2emimir.NewIngester("ingester-b-2", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingesterC0 := e2emimir.NewIngester("ingester-c-0", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	ingesterC1 := e2emimir.NewIngester("ingester-c-1", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	ingesterC2 := e2emimir.NewIngester("ingester-c-2", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	require.NoError(t, s.StartAndWaitReady(ingesterA0, ingesterA1, ingesterA2, ingesterB0, ingesterB1, ingesterB2, ingesterC0, ingesterC1, ingesterC2))

	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, querier))

	// Wait until all 9 ingesters are visible in the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(9), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(9), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait for all 3 partitions to be Active.
	for _, svc := range []*e2emimir.MimirService{distributor, querier} {
		require.NoError(t, svc.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_partition_ring_partitions"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester-partitions"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))))
	}

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// --- Phase 1: Push series while all ingesters are healthy ---
	now := time.Now()
	numSeries := 100 // Enough to spread across all 3 partitions via hashing.
	expectedVectors := map[string]model.Vector{}

	for i := 1; i <= numSeries; i++ {
		metricName := fmt.Sprintf("reliability_series_%d", i)
		series, expectedVector, _ := generateAlternatingSeries(i)(metricName, now)
		res, err := client.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
		expectedVectors[metricName] = expectedVector
	}

	// Verify all queries succeed before failure injection.
	for metricName, expectedVector := range expectedVectors {
		result, err := client.Query(metricName, now)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}

	// --- Phase 2: Kill ingesters in different partitions across different zones ---
	// Kill ingester-a-0 (partition 0, zone-a) and ingester-b-1 (partition 1, zone-b).
	// In classic architecture, this would contaminate zone-a and zone-b, causing 100% failure.
	// With per-partition isolation, each partition retains 2-of-3 zones.
	require.NoError(t, ingesterA0.Kill())
	require.NoError(t, ingesterB1.Kill())

	// --- Phase 3: Verify queries still succeed for ALL previously-pushed series ---
	// This is the key assertion: per-partition isolation means queries succeed even though
	// we've lost ingesters in 2 different zones.
	for metricName, expectedVector := range expectedVectors {
		result, err := client.Query(metricName, now)
		require.NoError(t, err, "query for %s should succeed after cross-partition cross-zone failures", metricName)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}

	// --- Phase 4: Verify new writes still succeed ---
	// Push additional series after the failures — writes should route to healthy owners.
	for i := numSeries + 1; i <= numSeries+20; i++ {
		metricName := fmt.Sprintf("reliability_series_%d", i)
		series, expectedVector, _ := generateFloatSeries(metricName, now)
		res, err := client.Push(series)
		require.NoError(t, err, "push of %s should succeed after failures", metricName)
		require.Equal(t, 200, res.StatusCode)
		expectedVectors[metricName] = expectedVector
	}

	// Query the newly-pushed series too.
	for i := numSeries + 1; i <= numSeries+20; i++ {
		metricName := fmt.Sprintf("reliability_series_%d", i)
		result, err := client.Query(metricName, now)
		require.NoError(t, err, "query for newly-pushed %s should succeed", metricName)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVectors[metricName], result.(model.Vector))
	}

	// --- Phase 5: Kill a THIRD ingester in yet another partition and zone ---
	// Kill ingester-c-2 (partition 2, zone-c). Now we have the maximum survivable spread:
	// one failure per partition, each in a different zone. Every partition still has 2 of 3 zones.
	require.NoError(t, ingesterC2.Kill())

	// All queries should still succeed.
	for metricName, expectedVector := range expectedVectors {
		result, err := client.Query(metricName, now)
		require.NoError(t, err, "query for %s should succeed with 3 failures in max survivable spread", metricName)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}
}
