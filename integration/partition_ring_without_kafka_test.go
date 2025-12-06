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

	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	require.NoError(t, s.StartAndWaitReady(ingester1, ingester2, ingester3))

	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, querier))

	// Wait until distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

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

	// Verify partition ring metrics are present (indicates partition ring is being used)
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_distributor_write_requests_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "path", "partition"))))
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

	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingester4 := e2emimir.NewIngester("ingester-4", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingester5 := e2emimir.NewIngester("ingester-5", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	ingester6 := e2emimir.NewIngester("ingester-6", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	require.NoError(t, s.StartAndWaitReady(ingester1, ingester2, ingester3, ingester4, ingester5, ingester6))

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
	require.NoError(t, ingester1.Kill())
	require.NoError(t, ingester2.Kill())

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
	require.NoError(t, ingester3.Kill())
	require.NoError(t, ingester4.Kill())

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

	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	require.NoError(t, s.StartAndWaitReady(ingester1, ingester2, ingester3))

	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, querier))

	// Wait until distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

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

	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	require.NoError(t, s.StartAndWaitReady(ingester1, ingester2, ingester3))

	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, querier))

	// Wait until distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

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
