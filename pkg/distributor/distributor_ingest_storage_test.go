// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/mtime"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

// kafkaTopic is the Kafka topic used for ingest storage tests.
const kafkaTopic = "test"

func TestDistributor_Push_ShouldSupportIngestStorage(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	// Mock distributor current time (used to get stable metrics assertion).
	now := time.Now()
	mtime.NowForce(now)
	t.Cleanup(mtime.NowReset)

	// To keep assertions simple, all tests send the same request.
	createRequest := func() *mimirpb.WriteRequest {
		return &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				makeTimeseries([]string{model.MetricNameLabel, "series_one"}, makeSamples(now.UnixMilli(), 1), nil, makeExemplars([]string{"trace_id", "xxx"}, now.UnixMilli(), 1)),
				makeTimeseries([]string{model.MetricNameLabel, "series_two"}, makeSamples(now.UnixMilli(), 2), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_three"}, makeSamples(now.UnixMilli(), 3), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_four"}, makeSamples(now.UnixMilli(), 4), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_five"}, makeSamples(now.UnixMilli(), 5), nil, nil),
			},
			Metadata: []*mimirpb.MetricMetadata{
				{MetricFamilyName: "series_one", Type: mimirpb.COUNTER, Help: "Series one description"},
				{MetricFamilyName: "series_two", Type: mimirpb.COUNTER, Help: "Series two description"},
			},
		}
	}

	tests := map[string]struct {
		shardSize                    int
		kafkaPartitionCustomResponse map[int32]*kmsg.ProduceResponse
		expectedErr                  error
		expectedSeriesByPartition    map[int32][]string
	}{
		"should shard series across all partitions when shuffle sharding is disabled": {
			shardSize: 0,
			expectedSeriesByPartition: map[int32][]string{
				0: {"series_four", "series_one", "series_three"},
				1: {"series_two"},
				2: {"series_five"},
			},
		},
		"should shard series across the number of configured partitions when shuffle sharding is enabled": {
			shardSize: 2,
			expectedSeriesByPartition: map[int32][]string{
				1: {"series_one", "series_three", "series_two"},
				2: {"series_five", "series_four"},
			},
		},
		"should return gRPC error if writing to 1 out of N partitions fail with a non-retryable error": {
			shardSize: 0,
			kafkaPartitionCustomResponse: map[int32]*kmsg.ProduceResponse{
				// Non-retryable error.
				1: testkafka.CreateProduceResponseError(0, kafkaTopic, 1, kerr.InvalidTopicException),
			},
			expectedErr: fmt.Errorf("%s 1", failedPushingToPartitionMessage),
			expectedSeriesByPartition: map[int32][]string{
				// Partition 1 is missing because it failed.
				0: {"series_four", "series_one", "series_three"},
				2: {"series_five"},
			},
		},

		// This test case simulate the case the request timeout is < than the Kafka writer timeout and producing
		// the message to Kafka fails consistently for a partition. In this case, the request will timeout before
		// Kafka writer and so the client will get a context.DeadlineExceeded.
		"should return context.DeadlineExceeded error if writing to 1 out of N partitions times out because of a retryable error": {
			shardSize: 0,
			kafkaPartitionCustomResponse: map[int32]*kmsg.ProduceResponse{
				// Retryable error.
				1: testkafka.CreateProduceResponseError(0, kafkaTopic, 1, kerr.LeaderNotAvailable),
			},
			expectedErr: context.DeadlineExceeded,
			expectedSeriesByPartition: map[int32][]string{
				// Partition 1 is missing because it failed.
				0: {"series_four", "series_one", "series_three"},
				2: {"series_five"},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			limits := prepareDefaultLimits()
			limits.IngestionPartitionsTenantShardSize = testData.shardSize
			limits.MaxGlobalExemplarsPerUser = 1000

			testConfig := prepConfig{
				numDistributors:         1,
				ingestStorageEnabled:    true,
				ingestStoragePartitions: 3,
				limits:                  limits,
				configure: func(cfg *Config) {
					// Run a number of clients equal to the number of partitions, so that each partition
					// has its own client, as requested by some test cases.
					cfg.IngestStorageConfig.KafkaConfig.WriteClients = 3
				},
			}

			distributors, _, regs, kafkaCluster := prepare(t, testConfig)
			require.Len(t, distributors, 1)
			require.Len(t, regs, 1)

			// Mock Kafka to fail specific partitions (if configured).
			kafkaCluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
				kafkaCluster.KeepControl()

				produceReq := request.(*kmsg.ProduceRequest)
				for _, topic := range produceReq.Topics {
					// For this test to work correctly we expect each request to write only to 1 partition,
					// because we'll fail the entire request.
					require.Len(t, topic.Partitions, 1)

					if res := testData.kafkaPartitionCustomResponse[topic.Partitions[0].Partition]; res != nil {
						res.SetVersion(request.GetVersion())
						// Copy the TopicID from the request to the response (required for produce v13+)
						if len(res.Topics) > 0 {
							res.Topics[0].TopicID = topic.TopicID
						}
						return res, nil, true
					}
				}

				return nil, nil, false
			})

			// Send write request.
			res, err := distributors[0].Push(ctx, createRequest())

			if testData.expectedErr != nil {
				require.Error(t, err)
				assert.Nil(t, res)

				if errors.Is(testData.expectedErr, context.DeadlineExceeded) {
					// The context.DeadlineExceeded is not expected to be wrapped in a gRPC error.
					assert.ErrorIs(t, err, testData.expectedErr)
				} else {
					// We expect a gRPC error.
					errStatus, ok := grpcutil.ErrorToStatus(err)
					require.True(t, ok)
					assert.Equal(t, codes.Internal, errStatus.Code())
					assert.ErrorContains(t, errStatus.Err(), testData.expectedErr.Error())
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, emptyResponse, res)
			}

			// Ensure series has been sharded as expected.
			actualSeriesByPartition := readAllMetricNamesByPartitionFromKafka(t, kafkaCluster.ListenAddrs(), testConfig.ingestStoragePartitions, time.Second)
			assert.Equal(t, testData.expectedSeriesByPartition, actualSeriesByPartition)

			// Asserts on tracked metrics.
			assert.NoError(t, testutil.GatherAndCompare(regs[0], strings.NewReader(fmt.Sprintf(`
					# HELP cortex_distributor_requests_in_total The total number of requests that have come in to the distributor, including rejected or deduped requests.
					# TYPE cortex_distributor_requests_in_total counter
					cortex_distributor_requests_in_total{user="user",version="1.0"} 1

					# HELP cortex_distributor_received_requests_total The total number of received requests, excluding rejected and deduped requests.
					# TYPE cortex_distributor_received_requests_total counter
					cortex_distributor_received_requests_total{user="user"} 1

					# HELP cortex_distributor_samples_in_total The total number of samples that have come in to the distributor, including rejected or deduped samples.
					# TYPE cortex_distributor_samples_in_total counter
					cortex_distributor_samples_in_total{user="user"} 5

					# HELP cortex_distributor_received_samples_total The total number of received samples, including native histogram samples, excluding rejected and deduped samples.
					# TYPE cortex_distributor_received_samples_total counter
					cortex_distributor_received_samples_total{user="user"} 5

					# HELP cortex_distributor_metadata_in_total The total number of metadata the have come in to the distributor, including rejected.
					# TYPE cortex_distributor_metadata_in_total counter
					cortex_distributor_metadata_in_total{user="user"} 2

					# HELP cortex_distributor_received_metadata_total The total number of received metadata, excluding rejected.
					# TYPE cortex_distributor_received_metadata_total counter
					cortex_distributor_received_metadata_total{user="user"} 2

					# HELP cortex_distributor_exemplars_in_total The total number of exemplars that have come in to the distributor, including rejected or deduped exemplars.
					# TYPE cortex_distributor_exemplars_in_total counter
					cortex_distributor_exemplars_in_total{user="user"} 1

					# HELP cortex_distributor_received_exemplars_total The total number of received exemplars, excluding rejected and deduped exemplars.
					# TYPE cortex_distributor_received_exemplars_total counter
					cortex_distributor_received_exemplars_total{user="user"} 1

					# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
					# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
					cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} %f
				`, float64(now.UnixMilli())/1000.)),
				"cortex_distributor_received_requests_total",
				"cortex_distributor_received_samples_total",
				"cortex_distributor_received_exemplars_total",
				"cortex_distributor_received_metadata_total",
				"cortex_distributor_requests_in_total",
				"cortex_distributor_samples_in_total",
				"cortex_distributor_exemplars_in_total",
				"cortex_distributor_metadata_in_total",
				"cortex_distributor_latest_seen_sample_timestamp_seconds",
			))
		})
	}
}

func TestDistributor_Push_ShouldReturnErrorMappedTo4xxStatusCodeIfWriteRequestContainsTimeseriesBiggerThanLimit(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	now := time.Now()

	hugeLabelValueLength := (1 << 24) - 1 // This is one character less than the maximum label length allowed by Prometheus.

	createWriteRequest := func() *mimirpb.WriteRequest {
		return &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				makeTimeseries([]string{model.MetricNameLabel, strings.Repeat("x", hugeLabelValueLength)}, makeSamples(now.UnixMilli(), 1), nil, nil),
			},
		}
	}

	limits := prepareDefaultLimits()
	limits.MaxLabelValueLength = hugeLabelValueLength

	overrides := validation.NewOverrides(*limits, nil)

	testConfig := prepConfig{
		numDistributors:         1,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 1,
		limits:                  limits,
	}

	distributors, _, regs, _ := prepare(t, testConfig)
	require.Len(t, distributors, 1)
	require.Len(t, regs, 1)

	t.Run("Push()", func(t *testing.T) {
		// Send write request.
		res, err := distributors[0].Push(ctx, createWriteRequest())
		require.Error(t, err)
		require.Nil(t, res)

		// We expect a gRPC error.
		errStatus, ok := grpcutil.ErrorToStatus(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, errStatus.Code())
		assert.ErrorContains(t, errStatus.Err(), ingest.ErrWriteRequestDataItemTooLarge.Error())

		// We expect the gRPC error to be detected as client error.
		assert.True(t, mimirpb.IsClientError(err))
	})

	t.Run("Handler()", func(t *testing.T) {
		marshalledReq, err := createWriteRequest().Marshal()
		require.NoError(t, err)

		maxRecvMsgSize := hugeLabelValueLength * 2
		resp := httptest.NewRecorder()
		sourceIPs, _ := middleware.NewSourceIPs("SomeField", "(.*)", false)

		// Send write request through the HTTP handler.
		h := Handler(maxRecvMsgSize, nil, sourceIPs, false, false, overrides, RetryConfig{}, distributors[0].PushWithMiddlewares, nil, log.NewNopLogger())
		h.ServeHTTP(resp, createRequest(t, marshalledReq))
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestDistributor_Push_ShouldSupportWriteBothToIngestersAndPartitions(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	now := time.Now()

	// To keep assertions simple, all tests send the same request.
	createRequest := func() *mimirpb.WriteRequest {
		return &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				makeTimeseries([]string{model.MetricNameLabel, "series_one"}, makeSamples(now.UnixMilli(), 1), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_two"}, makeSamples(now.UnixMilli(), 2), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_three"}, makeSamples(now.UnixMilli(), 3), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_four"}, makeSamples(now.UnixMilli(), 4), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_five"}, makeSamples(now.UnixMilli(), 5), nil, nil),
			},
		}
	}

	tests := map[string]struct {
		shardSize                     int
		shouldFailWritingToPartitions bool
		shouldFailWritingToIngesters  bool
		expectedErr                   string
		expectedMetricsByPartition    map[int32][]string
		expectedMetricsByIngester     map[string][]string
	}{
		"should shard series across all partitions when shuffle sharding is disabled": {
			shardSize: 0,
			expectedMetricsByPartition: map[int32][]string{
				0: {"series_four", "series_one", "series_three"},
				1: {"series_two"},
				2: {"series_five"},
			},
			expectedMetricsByIngester: map[string][]string{
				"ingester-0": {"series_four", "series_five"},
				"ingester-1": {"series_one", "series_two", "series_three"},
				"ingester-2": {},
			},
		},
		"should shard series across the number of configured partitions / ingesters when shuffle sharding is enabled": {
			shardSize: 2,
			expectedMetricsByPartition: map[int32][]string{
				1: {"series_one", "series_three", "series_two"},
				2: {"series_five", "series_four"},
			},
			expectedMetricsByIngester: map[string][]string{
				"ingester-0": {"series_four", "series_five"},
				"ingester-1": {"series_one", "series_two", "series_three"},
			},
		},
		"should return gRPC error if fails to write to ingesters": {
			shouldFailWritingToIngesters: true,
			expectedErr:                  failedPushingToIngesterMessage,
		},
		"should return gRPC error if fails to write to partitions": {
			shouldFailWritingToPartitions: true,
			expectedErr:                   failedPushingToPartitionMessage,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			// Pre-condition: ensure that sharding is different between ingesters and partitions.
			// This is required to ensure series are correctly sharded based on ingesters and partitions ring.
			// If the sharding is the same, then we have no guarantee it's actually working as expected.
			if len(testData.expectedMetricsByIngester) > 0 && len(testData.expectedMetricsByPartition) > 0 {
				actualPartitionsSharding := map[string][]string{}
				actualIngestersSharding := map[string][]string{}

				for partitionID, partitionMetrics := range testData.expectedMetricsByPartition {
					actualPartitionsSharding[strconv.Itoa(int(partitionID))] = slices.Clone(partitionMetrics)
					slices.Sort(actualPartitionsSharding[strconv.Itoa(int(partitionID))])
				}
				for ingesterID, ingesterMetrics := range testData.expectedMetricsByIngester {
					partitionID, err := ingest.IngesterPartitionID(ingesterID)
					require.NoError(t, err)

					actualIngestersSharding[strconv.Itoa(int(partitionID))] = slices.Clone(ingesterMetrics)
					slices.Sort(actualIngestersSharding[strconv.Itoa(int(partitionID))])
				}

				require.NotEqual(t, actualPartitionsSharding, actualIngestersSharding)
			}

			// Setup distributors and ingesters.
			limits := prepareDefaultLimits()
			limits.IngestionPartitionsTenantShardSize = testData.shardSize
			limits.IngestionTenantShardSize = testData.shardSize

			testConfig := prepConfig{
				numDistributors:         1,
				numIngesters:            3,
				happyIngesters:          3,
				replicationFactor:       1,
				ingesterIngestionType:   ingesterIngestionTypeGRPC, // Do not consume from Kafka. Partitions are asserted directly checking Kafka.
				ingestStorageEnabled:    true,
				ingestStoragePartitions: 3,
				limits:                  limits,
				configure: func(cfg *Config) {
					cfg.IngestStorageConfig.Migration.DistributorSendToIngestersEnabled = true
				},
			}

			distributors, ingesters, regs, kafkaCluster := prepare(t, testConfig)
			require.Len(t, distributors, 1)
			require.Len(t, ingesters, 3)
			require.Len(t, regs, 1)

			if testData.shouldFailWritingToPartitions {
				kafkaCluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
					kafkaCluster.KeepControl()

					produceReq := req.(*kmsg.ProduceRequest)
					partitionID := produceReq.Topics[0].Partitions[0].Partition
					res := testkafka.CreateProduceResponseError(req.GetVersion(), kafkaTopic, partitionID, kerr.InvalidTopicException)
					// Copy the TopicID from the request to the response (required for produce v13+)
					if len(res.Topics) > 0 {
						res.Topics[0].TopicID = produceReq.Topics[0].TopicID
					}

					return res, nil, true
				})
			}

			if testData.shouldFailWritingToIngesters {
				for _, ingester := range ingesters {
					ingester.happy = false
				}
			}

			// Send write request.
			_, err := distributors[0].Push(ctx, createRequest())

			if testData.expectedErr != "" {
				require.Error(t, err)

				// We expect a gRPC error.
				errStatus, ok := grpcutil.ErrorToStatus(err)
				require.True(t, ok)
				assert.Equal(t, codes.Internal, errStatus.Code())
				assert.ErrorContains(t, errStatus.Err(), testData.expectedErr)

				// End the test here.
				return
			}

			require.NoError(t, err)

			// Ensure series has been correctly sharded to partitions.
			actualSeriesByPartition := readAllMetricNamesByPartitionFromKafka(t, kafkaCluster.ListenAddrs(), testConfig.ingestStoragePartitions, time.Second)
			if !assert.Equal(t, testData.expectedMetricsByPartition, actualSeriesByPartition, "please report this failure in https://github.com/grafana/mimir/issues/9299") {
				// This test is sometimes flaky. Add a log line to help debug it.
				// Inspect the offsets of partitions in Kafka. There may be records, but we couldn't fetch them in the 1s timeout above.
				kafkaClient, err := kgo.NewClient(kgo.SeedBrokers(kafkaCluster.ListenAddrs()...))
				assert.NoError(t, err)
				offsets, err := kadm.NewClient(kafkaClient).ListEndOffsets(context.Background(), kafkaTopic)
				assert.NoError(t, err)
				t.Logf("Kafka topic %s end offsets: %#v", kafkaTopic, offsets)
			}

			// Ensure series have been correctly sharded to ingesters.
			for _, ingester := range ingesters {
				assert.ElementsMatchf(t, testData.expectedMetricsByIngester[ingester.instanceID()], ingester.metricNames(), "ingester ID: %s", ingester.instanceID())
			}
		})
	}
}

func TestDistributor_Push_ShouldCleanupWriteRequestAfterWritingBothToIngestersAndPartitions(t *testing.T) {
	t.Parallel()

	ctx := user.InjectOrgID(context.Background(), "user")
	now := time.Now()

	testConfig := prepConfig{
		numDistributors:         1,
		numIngesters:            3,
		happyIngesters:          3,
		replicationFactor:       3,
		ingesterIngestionType:   ingesterIngestionTypeGRPC, // Do not consume from Kafka in this test.
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 1,
		limits:                  prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.IngestStorageConfig.Migration.DistributorSendToIngestersEnabled = true
		},
	}

	distributors, ingesters, regs, kafkaCluster := prepare(t, testConfig)
	require.Len(t, distributors, 1)
	require.Len(t, ingesters, 3)
	require.Len(t, regs, 1)

	// In this test ingesters have been configured with RF=3. This means that the write request will succeed
	// once written to at least 2 out of 3 ingesters. We configure 1 ingester to block the Push() request, and
	// then we control when unblocking it.
	releaseSlowIngesterPush := make(chan struct{})
	ingesters[0].registerBeforePushHook(func(_ context.Context, _ *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error, bool) {
		<-releaseSlowIngesterPush
		return nil, nil, false
	})

	// Wrap the distributor Push() to inject a custom cleanup function, so that we can track when it gets called.
	pushCleanupCallsCount := atomic.NewInt64(0)
	origPushWithMiddlewares := distributors[0].PushWithMiddlewares
	distributors[0].PushWithMiddlewares = func(ctx context.Context, req *Request) error {
		req.AddCleanup(func() {
			pushCleanupCallsCount.Inc()
		})

		return origPushWithMiddlewares(ctx, req)
	}

	// Send write request.
	_, err := distributors[0].Push(ctx, &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries([]string{model.MetricNameLabel, "series_one"}, makeSamples(now.UnixMilli(), 1), nil, nil),
		},
	})
	require.NoError(t, err)

	// Since there's still 1 ingester in-flight request, we expect the cleanup function not being called yet.
	require.Equal(t, int64(0), pushCleanupCallsCount.Load())
	time.Sleep(time.Second)
	require.Equal(t, int64(0), pushCleanupCallsCount.Load())

	// Unblock the slow ingester.
	close(releaseSlowIngesterPush)

	// Now we expect the cleanup function being called as soon as the request to the slow ingester completes.
	test.Poll(t, time.Second, int64(1), func() interface{} {
		return pushCleanupCallsCount.Load()
	})

	// Ensure series has been correctly written to partitions.
	actualSeriesByPartition := readAllMetricNamesByPartitionFromKafka(t, kafkaCluster.ListenAddrs(), testConfig.ingestStoragePartitions, time.Second)
	assert.Equal(t, map[int32][]string{0: {"series_one"}}, actualSeriesByPartition)

	// Ensure series have been correctly sharded to ingesters.
	for _, ingester := range ingesters {
		assert.Equal(t, []string{"series_one"}, ingester.metricNames(), "ingester ID: %s", ingester.instanceID())
	}
}

func TestDistributor_Push_IgnoreIngestStorageErrorsDuringMigration(t *testing.T) {
	t.Parallel()

	ctx := user.InjectOrgID(context.Background(), "user")
	now := time.Now()

	tests := map[string]struct {
		shouldFailIngester       bool
		shouldFailIngestStorage  bool
		ignoreIngestStorageError bool
		expectedErrorContext     string
		maxWaitTime              time.Duration
	}{
		"should give precedence to ingester error when both ingester and ingest storage errors occur and IgnoreIngestStorageError is enabled": {
			shouldFailIngester:       true,
			shouldFailIngestStorage:  true,
			ignoreIngestStorageError: true,
			expectedErrorContext:     "send data to ingesters",
		},
		"should succeed when only ingest storage errors occur and IgnoreIngestStorageError is enabled": {
			shouldFailIngester:       false,
			shouldFailIngestStorage:  true,
			ignoreIngestStorageError: true,
			expectedErrorContext:     "",
		},
		"should fail with timeout from partitionErrors when ignoreIngestStorageError is disabled and IngestStorageMaxWaitTime is set": {
			shouldFailIngester:       true,
			shouldFailIngestStorage:  true,
			ignoreIngestStorageError: false,
			expectedErrorContext:     "timeout",
			maxWaitTime:              200 * time.Millisecond,
		},
		"should succeed when only ingest storage errors occur and IgnoreIngestStorageError is enabled with IngestStorageMaxWaitTime is set": {
			shouldFailIngester:       false,
			shouldFailIngestStorage:  true,
			ignoreIngestStorageError: true,
			expectedErrorContext:     "",
			maxWaitTime:              200 * time.Millisecond,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			// Setup test configuration
			testConfig := prepConfig{
				numDistributors:         1,
				numIngesters:            1,
				happyIngesters:          1,
				replicationFactor:       1,
				ingesterIngestionType:   ingesterIngestionTypeGRPC,
				ingestStorageEnabled:    true,
				ingestStoragePartitions: 1,
				limits:                  prepareDefaultLimits(),
				configure: func(cfg *Config) {
					cfg.IngestStorageConfig.Migration.DistributorSendToIngestersEnabled = true
					cfg.IngestStorageConfig.Migration.IgnoreIngestStorageErrors = testData.ignoreIngestStorageError
					cfg.IngestStorageConfig.Migration.IngestStorageMaxWaitTime = testData.maxWaitTime
				},
			}

			distributors, ingesters, _, kafkaCluster := prepare(t, testConfig)

			require.Len(t, distributors, 1)
			require.Len(t, ingesters, 1)

			releaseProduceRequest := make(chan struct{})

			// Configure Kafka to return error if specified
			if testData.shouldFailIngestStorage {
				kafkaCluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
					kafkaCluster.KeepControl()
					<-releaseProduceRequest
					time.Sleep(time.Second)

					partitionID := req.(*kmsg.ProduceRequest).Topics[0].Partitions[0].Partition
					res := testkafka.CreateProduceResponseError(req.GetVersion(), kafkaTopic, partitionID, kerr.InvalidTopicException)

					return res, nil, true
				})
			}
			// Mock Kafka to return a hard error.
			if testData.shouldFailIngester {
				ingesters[0].registerBeforePushHook(func(_ context.Context, _ *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error, bool) {
					// Release the Kafka produce request once the push to ingester has been received.
					close(releaseProduceRequest)
					ingesterError := httpgrpc.Errorf(http.StatusBadRequest, "ingester error")
					return &mimirpb.WriteResponse{}, ingesterError, true
				})
			}

			// Send write request
			_, err := distributors[0].Push(ctx, &mimirpb.WriteRequest{
				Timeseries: []mimirpb.PreallocTimeseries{
					makeTimeseries([]string{model.MetricNameLabel, "series_one"}, makeSamples(now.UnixMilli(), 1), nil, nil),
				},
			})

			if testData.expectedErrorContext != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, testData.expectedErrorContext)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDistributor_Push_ShouldGivePrecedenceToPartitionsErrorWhenWritingBothToIngestersAndPartitions(t *testing.T) {
	t.Parallel()

	ctx := user.InjectOrgID(context.Background(), "user")
	now := time.Now()

	testConfig := prepConfig{
		numDistributors:         1,
		numIngesters:            1,
		happyIngesters:          1,
		replicationFactor:       1,
		ingesterIngestionType:   ingesterIngestionTypeGRPC, // Do not consume from Kafka in this test.
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 1,
		limits:                  prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.IngestStorageConfig.Migration.DistributorSendToIngestersEnabled = true
		},
	}

	distributors, ingesters, regs, kafkaCluster := prepare(t, testConfig)
	require.Len(t, distributors, 1)
	require.Len(t, ingesters, 1)
	require.Len(t, regs, 1)

	// Mock Kafka to return a hard error.
	releaseProduceRequest := make(chan struct{})
	kafkaCluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		kafkaCluster.KeepControl()

		// Wait until released, then add an extra sleep to increase the likelihood this error
		// will be returned after the ingester one.
		<-releaseProduceRequest
		time.Sleep(time.Second)

		partitionID := req.(*kmsg.ProduceRequest).Topics[0].Partitions[0].Partition
		res := testkafka.CreateProduceResponseError(req.GetVersion(), kafkaTopic, partitionID, kerr.InvalidTopicException)

		return res, nil, true
	})

	// Mock ingester to return a soft error.
	ingesters[0].registerBeforePushHook(func(_ context.Context, _ *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error, bool) {
		// Release the Kafka produce request once the push to ingester has been received.
		close(releaseProduceRequest)

		ingesterErr := httpgrpc.Errorf(http.StatusBadRequest, "ingester error")
		return &mimirpb.WriteResponse{}, ingesterErr, true
	})

	// Send write request.
	_, err := distributors[0].Push(ctx, &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries([]string{model.MetricNameLabel, "series_one"}, makeSamples(now.UnixMilli(), 1), nil, nil),
		},
	})

	require.Error(t, err)
	assert.ErrorContains(t, err, "send data to partitions")
}

func TestDistributor_UserStats_ShouldSupportIngestStorage(t *testing.T) {
	const preferredZone = "zone-a"

	tests := map[string]struct {
		ingesterStateByZone map[string]ingesterZoneState
		ingesterDataByZone  map[string][]*mimirpb.WriteRequest
		shardSize           int
		expectedSeries      uint64
		expectedErr         error
	}{
		"partitions RF=1 (1 zone), 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 3, happyIngesters: 3},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1"),
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3"),
				},
			},
			expectedSeries: 3,
		},
		"partitions RF=1 (1 zone), 6 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1"),
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_7"),
					makeWriteRequest(0, 1, 0, false, false, "series_8", "series_9"),
				},
			},
			expectedSeries: 9,
		},
		"partitions RF=1 (1 zone), 6 ingesters, 1 ingester in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_7"),
					makeWriteRequest(0, 1, 0, false, false, "series_8", "series_9"),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"partitions RF=1 (1 zone), 6 ingesters, 1 ingester is UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1"),
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_7"),
					nil,
				},
			},
			expectedErr: errFail,
		},
		"partitions RF=2 (2 zones), 4 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeries: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the preferred zone (zone-a) are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeries: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the non-preferred zone (zone-b) are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeries: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning different partitions are in LEAVING state across both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeries: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are in LEAVING state in both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the preferred zone (zone-a) are UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 0},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					nil,
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeries: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the non-preferred zone (zone-b) are UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 0},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					nil,
					nil,
				},
			},
			expectedSeries: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning different partitions are UNHEALTHY across both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					nil,
				},
			},
			expectedSeries: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are UNHEALTHY in both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					nil,
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					nil,
				},
			},
			expectedErr: errFail,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are UNHEALTHY in both zones but the partition is not part of the tenant's shard": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
				"zone-b": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
			},
			shardSize:      1, // Tenant's shard made of: partition 1.
			expectedSeries: 3,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			for _, minimizeIngesterRequests := range []bool{false, true} {
				t.Run(fmt.Sprintf("minimize ingester requests: %t", minimizeIngesterRequests), func(t *testing.T) {
					t.Parallel()

					// Create distributor
					distributors, _, _, _ := prepare(t, prepConfig{
						numDistributors:      1,
						ingesterStateByZone:  testData.ingesterStateByZone,
						ingesterDataByZone:   testData.ingesterDataByZone,
						ingestStorageEnabled: true,
						configure: func(config *Config) {
							config.PreferAvailabilityZones = []string{preferredZone}
							config.MinimizeIngesterRequests = minimizeIngesterRequests
						},
						limits: func() *validation.Limits {
							limits := prepareDefaultLimits()
							limits.IngestionPartitionsTenantShardSize = testData.shardSize
							return limits
						}(),
					})

					// Fetch user stats.
					ctx := user.InjectOrgID(context.Background(), "test")
					res, err := distributors[0].UserStats(ctx, cardinality.InMemoryMethod)

					if testData.expectedErr != nil {
						require.ErrorIs(t, err, testData.expectedErr)
						return
					}

					require.NoError(t, err)
					assert.Equal(t, testData.expectedSeries, res.NumSeries)
				})
			}
		})
	}
}

func TestDistributor_LabelValuesCardinality_AvailabilityAndConsistencyWithIngestStorage(t *testing.T) {
	const preferredZone = "zone-a"

	var (
		// Define fixtures used in tests.
		series1 = makeTimeseries([]string{model.MetricNameLabel, "series_1", "job", "job-a", "service", "service-1"}, makeSamples(0, 0), nil, nil)
		series2 = makeTimeseries([]string{model.MetricNameLabel, "series_2", "job", "job-b", "service", "service-1"}, makeSamples(0, 0), nil, nil)
		series3 = makeTimeseries([]string{model.MetricNameLabel, "series_3", "job", "job-c", "service", "service-1"}, makeSamples(0, 0), nil, nil)
		series4 = makeTimeseries([]string{model.MetricNameLabel, "series_4", "job", "job-a", "service", "service-1"}, makeSamples(0, 0), nil, nil)
		series5 = makeTimeseries([]string{model.MetricNameLabel, "series_5", "job", "job-a", "service", "service-2"}, makeSamples(0, 0), nil, nil)
		series6 = makeTimeseries([]string{model.MetricNameLabel, "series_6", "job", "job-b" /* no service label */}, makeSamples(0, 0), nil, nil)

		// To keep assertions simple, all tests push all series, and then request the cardinality of the same label names,
		// so we expect the same response from each successful test.
		reqLabelNames = []model.LabelName{"job", "service"}
		expectedRes   = []*client.LabelValueSeriesCount{
			{
				LabelName:        "job",
				LabelValueSeries: map[string]uint64{"job-a": 3, "job-b": 2, "job-c": 1},
			}, {
				LabelName:        "service",
				LabelValueSeries: map[string]uint64{"service-1": 4, "service-2": 1},
			},
		}
	)

	tests := map[string]struct {
		ingesterStateByZone map[string]ingesterZoneState
		ingesterDataByZone  map[string][]*mimirpb.WriteRequest
		shardSize           int
		expectedErr         error
	}{
		"partitions RF=1 (1 zone), 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 3, happyIngesters: 3},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequestWith(series1, series2),
					makeWriteRequestWith(series3, series4),
					makeWriteRequestWith(series5, series6),
				},
			},
		},
		"partitions RF=1 (1 zone), 6 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequestWith(series1),
					makeWriteRequestWith(series2),
					makeWriteRequestWith(series3),
					makeWriteRequestWith(series4),
					makeWriteRequestWith(series5),
					makeWriteRequestWith(series6),
				},
			},
		},
		"partitions RF=1 (1 zone), 6 ingesters, 1 ingester in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					nil,
					makeWriteRequestWith(series2),
					makeWriteRequestWith(series3),
					makeWriteRequestWith(series4),
					makeWriteRequestWith(series5),
					makeWriteRequestWith(series6),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"partitions RF=1 (1 zone), 6 ingesters, 1 ingester is UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequestWith(series1),
					makeWriteRequestWith(series2),
					makeWriteRequestWith(series3),
					makeWriteRequestWith(series4),
					makeWriteRequestWith(series5),
					nil,
				},
			},
			expectedErr: errFail,
		},
		"partitions RF=2 (2 zones), 4 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
			},
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the preferred zone (zone-a) are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
			},
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the non-preferred zone (zone-b) are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
			},
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning different partitions are in LEAVING state across both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
			},
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are in LEAVING state in both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the preferred zone (zone-a) are UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 0},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					nil,
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
			},
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the non-preferred zone (zone-b) are UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 0},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
				"zone-b": {
					nil,
					nil,
				},
			},
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning different partitions are UNHEALTHY across both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					makeWriteRequestWith(series5, series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					nil,
				},
			},
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are UNHEALTHY in both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4),
					nil,
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					nil,
				},
			},
			expectedErr: errFail,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are UNHEALTHY in both zones but the partition is not part of the tenant's shard": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
				},
				"zone-b": {
					nil,
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
				},
			},
			shardSize: 1, // Tenant's shard made of: partition 1.
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			for _, minimizeIngesterRequests := range []bool{false, true} {
				t.Run(fmt.Sprintf("minimize ingester requests: %t", minimizeIngesterRequests), func(t *testing.T) {
					t.Parallel()

					// Create distributor
					distributors, _, _, _ := prepare(t, prepConfig{
						numDistributors:      1,
						ingesterStateByZone:  testData.ingesterStateByZone,
						ingesterDataByZone:   testData.ingesterDataByZone,
						ingestStorageEnabled: true,
						configure: func(config *Config) {
							config.PreferAvailabilityZones = []string{preferredZone}
							config.MinimizeIngesterRequests = minimizeIngesterRequests
						},
						limits: func() *validation.Limits {
							limits := prepareDefaultLimits()
							limits.IngestionPartitionsTenantShardSize = testData.shardSize
							return limits
						}(),
					})

					// Fetch label values cardinality.
					ctx := user.InjectOrgID(context.Background(), "test")
					_, res, err := distributors[0].LabelValuesCardinality(ctx, reqLabelNames, nil, cardinality.InMemoryMethod)

					if testData.expectedErr != nil {
						require.ErrorIs(t, err, testData.expectedErr)
						return
					}

					require.NoError(t, err)
					assert.ElementsMatch(t, expectedRes, res.Items)
				})
			}
		})
	}
}

func TestDistributor_ActiveSeries_AvailabilityAndConsistencyWithIngestStorage(t *testing.T) {
	const preferredZone = "zone-a"

	// In this test we run all queries with a matcher which matches all series.
	reqMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+")}

	tests := map[string]struct {
		ingesterStateByZone map[string]ingesterZoneState
		ingesterDataByZone  map[string][]*mimirpb.WriteRequest
		shardSize           int
		expectedSeriesCount int
		expectedErr         error
	}{
		"partitions RF=1 (1 zone), 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 3, happyIngesters: 3},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1"),
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3"),
				},
			},
			expectedSeriesCount: 3,
		},
		"partitions RF=1 (1 zone), 6 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1"),
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_7"),
					makeWriteRequest(0, 1, 0, false, false, "series_8", "series_9"),
				},
			},
			expectedSeriesCount: 9,
		},
		"partitions RF=1 (1 zone), 6 ingesters, 1 ingester in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_7"),
					makeWriteRequest(0, 1, 0, false, false, "series_8", "series_9"),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"partitions RF=1 (1 zone), 6 ingesters, 1 ingester is UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1"),
					makeWriteRequest(0, 1, 0, false, false, "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_7"),
					nil,
				},
			},
			expectedErr: errFail,
		},
		"partitions RF=2 (2 zones), 4 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeriesCount: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the preferred zone (zone-a) are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeriesCount: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the non-preferred zone (zone-b) are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeriesCount: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning different partitions are in LEAVING state across both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeriesCount: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are in LEAVING state in both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the preferred zone (zone-a) are UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 0},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					nil,
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
			},
			expectedSeriesCount: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, all ingesters in the non-preferred zone (zone-b) are UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 0},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					nil,
					nil,
				},
			},
			expectedSeriesCount: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning different partitions are UNHEALTHY across both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					nil,
				},
			},
			expectedSeriesCount: 5,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are UNHEALTHY in both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					nil,
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					nil,
				},
			},
			expectedErr: errFail,
		},
		"partitions RF=2 (2 zones), 4 ingesters, ingesters owning the same partition are UNHEALTHY in both zones but the partition is not part of the tenant's shard": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
				"zone-b": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
			},
			shardSize:           1, // Tenant's shard made of: partition 1.
			expectedSeriesCount: 3,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			for _, minimizeIngesterRequests := range []bool{false, true} {
				t.Run(fmt.Sprintf("minimize ingester requests: %t", minimizeIngesterRequests), func(t *testing.T) {
					t.Parallel()

					// Create distributor.
					distributors, _, _, _ := prepare(t, prepConfig{
						ingesterStateByZone:  testData.ingesterStateByZone,
						ingesterDataByZone:   testData.ingesterDataByZone,
						numDistributors:      1,
						ingestStorageEnabled: true,
						configure: func(config *Config) {
							config.MinimizeIngesterRequests = minimizeIngesterRequests
							config.PreferAvailabilityZones = []string{preferredZone}
						},
						limits: func() *validation.Limits {
							limits := prepareDefaultLimits()
							limits.IngestionPartitionsTenantShardSize = testData.shardSize
							return limits
						}(),
					})

					ctx := user.InjectOrgID(context.Background(), "test")
					qStats, ctx := stats.ContextWithEmptyStats(ctx)

					// Query active series.
					series, err := distributors[0].ActiveSeries(ctx, reqMatchers)
					if testData.expectedErr != nil {
						require.ErrorIs(t, err, testData.expectedErr)
						return
					}

					require.NoError(t, err)
					assert.Equal(t, testData.expectedSeriesCount, len(series))

					// Check that query stats are set correctly.
					assert.Equal(t, testData.expectedSeriesCount, int(qStats.GetFetchedSeriesCount()))
				})
			}
		})
	}
}

func readAllRecordsFromKafka(t testing.TB, kafkaAddresses []string, numPartitions int32, timeout time.Duration) []*kgo.Record {
	// Read all partitions from the beginning.
	offsets := make(map[int32]kgo.Offset, numPartitions)
	for partitionID := int32(0); partitionID < numPartitions; partitionID++ {
		offsets[partitionID] = kgo.NewOffset().AtStart()
	}

	// Init the client.
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaAddresses...),
		// Override the default retry backoff to quickly retry Fetches.
		kgo.RetryBackoffFn(func(_ int) time.Duration { return 10 * time.Millisecond }),
		// Set a low FetchMaxWait, because we prefer to retry the Fetch rather than hanging on it,
		// in case the Fetch is stuck on the server side (fake Kafka) for any reason.
		kgo.FetchMaxWait(timeout/4),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			kafkaTopic: offsets,
		}))

	require.NoError(t, err)
	t.Cleanup(kafkaClient.Close)

	var records []*kgo.Record

	// Read all records until no data has been received for at least the timeout period.
	// We don't stop reading as soon as the expected number of entries has been found
	// because we also want to make sure no more than expected entries are written to Kafka.
	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		// Fetch all buffered records.
		fetches := kafkaClient.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}

			t.Fatal(err)
		}

		fetches.EachRecord(func(record *kgo.Record) {
			records = append(records, record)
		})
	}

	return records
}

func readAllRequestsByPartitionFromKafka(t testing.TB, kafkaAddresses []string, numPartitions int32, timeout time.Duration) map[int32][]*mimirpb.WriteRequest {
	requestsByPartition := make(map[int32][]*mimirpb.WriteRequest, numPartitions)
	records := readAllRecordsFromKafka(t, kafkaAddresses, numPartitions, timeout)

	for _, record := range records {
		req := &mimirpb.WriteRequest{}
		require.NoError(t, req.Unmarshal(record.Value))

		requestsByPartition[record.Partition] = append(requestsByPartition[record.Partition], req)
	}

	return requestsByPartition
}

func readAllMetricNamesByPartitionFromKafka(t testing.TB, kafkaAddresses []string, numPartitions int32, timeout time.Duration) map[int32][]string {
	requestsByPartition := readAllRequestsByPartitionFromKafka(t, kafkaAddresses, numPartitions, timeout)
	actualSeriesByPartition := map[int32][]string{}

	for partitionID, requests := range requestsByPartition {
		for _, req := range requests {
			for _, series := range req.Timeseries {
				metricName, err := extract.UnsafeMetricNameFromLabelAdapters(series.Labels)
				require.NoError(t, err)

				actualSeriesByPartition[partitionID] = append(actualSeriesByPartition[partitionID], metricName)
			}
		}

		slices.Sort(actualSeriesByPartition[partitionID])
	}

	return actualSeriesByPartition
}

func TestDistributor_Push_WriteToPartitionOwners(t *testing.T) {
	// This test verifies that when Kafka is disabled, the distributor writes directly
	// to partition owners (ingesters) using zone-aware quorum semantics.
	ctx := user.InjectOrgID(context.Background(), "user")

	now := time.Now()
	mtime.NowForce(now)
	t.Cleanup(mtime.NowReset)

	createRequest := func() *mimirpb.WriteRequest {
		return &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				makeTimeseries([]string{model.MetricNameLabel, "series_one"}, makeSamples(now.UnixMilli(), 1), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_two"}, makeSamples(now.UnixMilli(), 2), nil, nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_three"}, makeSamples(now.UnixMilli(), 3), nil, nil),
			},
		}
	}

	tests := map[string]struct {
		ingesterStateByZone map[string]ingesterZoneState
		expectedErr         error
		expectedPushedZones int // Minimum number of zones that should receive pushes
	}{
		"should successfully push to partition owners with 3 zones (2 of 3 quorum)": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 1, happyIngesters: 1},
				"zone-b": {numIngesters: 1, happyIngesters: 1},
				"zone-c": {numIngesters: 1, happyIngesters: 1},
			},
			expectedPushedZones: 2, // At least 2 of 3 zones should receive push
		},
		"should successfully push to partition owners with 2 zones (2 of 2 quorum)": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 1, happyIngesters: 1},
				"zone-b": {numIngesters: 1, happyIngesters: 1},
			},
			expectedPushedZones: 2, // Both zones required
		},
		"should fail with single zone when RF=3 (cold start - need 2 zones for quorum)": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 1, happyIngesters: 1},
			},
			// With RF=3 (default), quorum requires 2 zones. Single zone cannot achieve quorum.
			// This is the expected cold-start behavior: writes rejected until enough zones are up.
			expectedErr: fmt.Errorf("insufficient healthy zones for quorum (have 1, need 2 of 3)"),
		},
		"should fail when one zone is unhealthy in 2-zone setup (no quorum)": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 1, happyIngesters: 1},
				"zone-b": {numIngesters: 1, happyIngesters: 0}, // Unhappy - will fail on push
			},
			// With 2 zones and quorum requirement of 2, failure to push to one zone
			// results in not meeting quorum, causing the overall push to fail.
			expectedErr: fmt.Errorf("failed pushing to ingester"),
		},
		"should succeed when one zone is unhealthy in 3-zone setup (still has quorum)": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 1, happyIngesters: 1},
				"zone-b": {numIngesters: 1, happyIngesters: 1},
				"zone-c": {numIngesters: 1, happyIngesters: 0}, // Unhappy - but 2 of 3 is quorum
			},
			expectedPushedZones: 2,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			limits := prepareDefaultLimits()

			testConfig := prepConfig{
				numDistributors:         1,
				ingestStorageEnabled:    true,
				ingestStoragePartitions: 1, // Single partition for simplicity
				ingesterStateByZone:     testData.ingesterStateByZone,
				ingesterIngestionType:   ingesterIngestionTypeGRPC, // Direct push, not Kafka
				limits:                  limits,
				configure: func(cfg *Config) {
					// Disable Kafka to use writeToPartitionOwners
					cfg.IngestStorageConfig.KafkaConfig.Enabled = false
					cfg.IngestStorageConfig.KafkaConfig.Address = ""
					cfg.IngestStorageConfig.Migration.WritePercentage = 100
				},
			}

			distributors, ingesters, _, _ := prepare(t, testConfig)
			require.Len(t, distributors, 1)

			// Send write request
			res, err := distributors[0].Push(ctx, createRequest())

			if testData.expectedErr != nil {
				require.Error(t, err)
				require.Nil(t, res)
				assert.ErrorContains(t, err, testData.expectedErr.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, emptyResponse, res)

				// Count how many zones received pushes
				zonesWithPushes := make(map[string]bool)
				for _, ing := range ingesters {
					if countCalls(ingesters, "Push") > 0 && ing.timeseries != nil && len(ing.timeseries) > 0 {
						zonesWithPushes[ing.zone] = true
					}
				}

				// Verify at least the expected number of zones received data
				assert.GreaterOrEqual(t, len(zonesWithPushes), testData.expectedPushedZones,
					"expected at least %d zones to receive pushes, got %d", testData.expectedPushedZones, len(zonesWithPushes))
			}
		})
	}
}

// TestDistributor_WriteToPartitionOwners_OwnerNotInIngesterRing verifies that writeToPartitionOwners
// correctly skips owners that are unhealthy in the ingester ring (LEAVING or JOINING state) and
// still achieves quorum with the remaining healthy owners.
func TestDistributor_WriteToPartitionOwners_OwnerNotInIngesterRing(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	now := time.Now()
	mtime.NowForce(now)
	t.Cleanup(mtime.NowReset)

	createRequest := func() *mimirpb.WriteRequest {
		return &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				makeTimeseries([]string{model.MetricNameLabel, "series_one"}, makeSamples(now.UnixMilli(), 1), nil, nil),
			},
		}
	}

	tests := map[string]struct {
		ingesterStateByZone map[string]ingesterZoneState
		expectedErr         string // substring expected in error, empty means success
		expectZoneCPush     bool   // whether zone-c ingester should receive a Push call
	}{
		"one zone LEAVING is skipped, quorum met with remaining 2 zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateHappy}},
				"zone-c": {states: []ingesterState{ingesterStateHappy}, ringStates: []ring.InstanceState{ring.LEAVING}},
			},
			// zone-c is LEAVING so IsHealthy(Write) returns false → skipped.
			// 2 healthy zones remain, RF=3 needs quorum of 2 → succeeds.
			expectedErr:     "",
			expectZoneCPush: false,
		},
		"two zones LEAVING leaves only 1 healthy zone, quorum lost": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateHappy}, ringStates: []ring.InstanceState{ring.LEAVING}},
				"zone-c": {states: []ingesterState{ingesterStateHappy}, ringStates: []ring.InstanceState{ring.LEAVING}},
			},
			// zone-b and zone-c skipped. Only 1 healthy zone. RF=3 needs quorum of 2 → rejected.
			expectedErr:     "insufficient healthy zones for quorum (have 1, need 2 of 3)",
			expectZoneCPush: false,
		},
		"one zone JOINING is also skipped for Write, quorum met": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateHappy}},
				"zone-c": {states: []ingesterState{ingesterStateHappy}, ringStates: []ring.InstanceState{ring.JOINING}},
			},
			// JOINING state is not healthy for Write → skipped just like LEAVING.
			expectedErr:     "",
			expectZoneCPush: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			testConfig := prepConfig{
				numDistributors:         1,
				ingestStorageEnabled:    true,
				ingestStoragePartitions: 1,
				ingesterStateByZone:     testData.ingesterStateByZone,
				ingesterIngestionType:   ingesterIngestionTypeGRPC,
				limits:                  prepareDefaultLimits(),
				configure: func(cfg *Config) {
					cfg.IngestStorageConfig.KafkaConfig.Enabled = false
					cfg.IngestStorageConfig.KafkaConfig.Address = ""
					cfg.IngestStorageConfig.Migration.WritePercentage = 100
				},
			}

			distributors, ingesters, _, _ := prepare(t, testConfig)
			require.Len(t, distributors, 1)

			_, err := distributors[0].Push(ctx, createRequest())

			if testData.expectedErr != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, testData.expectedErr)
			} else {
				require.NoError(t, err)

				// Verify zone-a and zone-b both received pushes (quorum requires both).
				zonesWithPushes := make(map[string]bool)
				for _, ing := range ingesters {
					if ing.countCalls("Push") > 0 {
						zonesWithPushes[ing.zone] = true
					}
				}
				assert.True(t, zonesWithPushes["zone-a"], "zone-a must receive push for quorum")
				assert.True(t, zonesWithPushes["zone-b"], "zone-b must receive push for quorum")
			}

			// Verify zone-c push expectation (skipped or not).
			for _, ing := range ingesters {
				if ing.zone == "zone-c" {
					if testData.expectZoneCPush {
						assert.Greater(t, ing.countCalls("Push"), 0, "zone-c should have received a push")
					} else {
						assert.Equal(t, 0, ing.countCalls("Push"), "zone-c should have been skipped (unhealthy in ring)")
					}
				}
			}
		})
	}
}

// TestPartition_IngesterRestart_RejoinsPartition verifies that after an ingester transiently fails
// (simulating a crash), subsequent writes succeed once the ingester recovers, and the recovered
// ingester receives data again.
func TestPartition_IngesterRestart_RejoinsPartition(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	now := time.Now()
	mtime.NowForce(now)
	t.Cleanup(mtime.NowReset)

	createRequest := func(name string) *mimirpb.WriteRequest {
		return &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				makeTimeseries([]string{model.MetricNameLabel, name}, makeSamples(now.UnixMilli(), 1), nil, nil),
			},
		}
	}

	testConfig := prepConfig{
		numDistributors:         1,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 1,
		ingesterStateByZone: map[string]ingesterZoneState{
			"zone-a": {numIngesters: 1, happyIngesters: 1},
			"zone-b": {numIngesters: 1, happyIngesters: 1},
			"zone-c": {numIngesters: 1, happyIngesters: 1},
		},
		ingesterIngestionType: ingesterIngestionTypeGRPC,
		limits:                prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.IngestStorageConfig.KafkaConfig.Enabled = false
			cfg.IngestStorageConfig.KafkaConfig.Address = ""
			cfg.IngestStorageConfig.Migration.WritePercentage = 100
		},
	}

	distributors, ingesters, _, _ := prepare(t, testConfig)
	require.Len(t, distributors, 1)

	// Find the zone-c ingester and install a hook that fails the first Push call (simulates crash).
	var zoneCIngester *mockIngester
	for _, ing := range ingesters {
		if ing.zone == "zone-c" {
			zoneCIngester = ing
			break
		}
	}
	require.NotNil(t, zoneCIngester)

	var mu sync.Mutex
	crashed := false
	zoneCIngester.registerBeforePushHook(func(_ context.Context, _ *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error, bool) {
		mu.Lock()
		defer mu.Unlock()
		if !crashed {
			crashed = true
			return nil, errors.New("ingester crashed"), true
		}
		// Subsequent calls: let normal push processing happen.
		return nil, nil, false
	})

	// First push: zone-c fails but 2-of-3 quorum (zone-a + zone-b) succeeds.
	res, err := distributors[0].Push(ctx, createRequest("series_before_crash"))
	require.NoError(t, err)
	assert.Equal(t, emptyResponse, res)

	// zone-c was called but returned an error — verify it received no data.
	assert.Equal(t, 0, len(zoneCIngester.timeseries), "zone-c should have no data after crash")

	// Second push: zone-c's hook now passes through to normal processing.
	res, err = distributors[0].Push(ctx, createRequest("series_after_recovery"))
	require.NoError(t, err)
	assert.Equal(t, emptyResponse, res)

	// After recovery, zone-c should have received the second push's data.
	assert.Greater(t, len(zoneCIngester.timeseries), 0, "zone-c should have data after recovery")

	// Verify all 3 zones received data across both pushes.
	for _, ing := range ingesters {
		assert.Greater(t, ing.countCalls("Push"), 0, "ingester in %s should have been called", ing.zone)
	}
}

// TestPartition_NetworkPartition_ZoneIsolation verifies behavior when a zone becomes isolated
// due to network partitioning. With 3 zones, losing 1 zone should still allow writes to succeed.
func TestPartition_NetworkPartition_ZoneIsolation(t *testing.T) {
	// This test simulates network partition by making one zone's ingesters unhappy (fail on push).
	// With 3 zones and quorum of 2, writes should still succeed.
	ctx := user.InjectOrgID(context.Background(), "user")

	now := time.Now()
	mtime.NowForce(now)
	t.Cleanup(mtime.NowReset)

	createRequest := func() *mimirpb.WriteRequest {
		return &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				makeTimeseries([]string{model.MetricNameLabel, "series_one"}, makeSamples(now.UnixMilli(), 1), nil, nil),
			},
		}
	}

	limits := prepareDefaultLimits()

	// Zone-c is "partitioned" (unhappy ingesters simulate network isolation)
	testConfig := prepConfig{
		numDistributors:         1,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 1,
		ingesterStateByZone: map[string]ingesterZoneState{
			"zone-a": {numIngesters: 1, happyIngesters: 1},
			"zone-b": {numIngesters: 1, happyIngesters: 1},
			"zone-c": {numIngesters: 1, happyIngesters: 0}, // Simulates network partition
		},
		ingesterIngestionType: ingesterIngestionTypeGRPC,
		limits:                limits,
		configure: func(cfg *Config) {
			cfg.IngestStorageConfig.KafkaConfig.Enabled = false
			cfg.IngestStorageConfig.KafkaConfig.Address = ""
			cfg.IngestStorageConfig.Migration.WritePercentage = 100
		},
	}

	distributors, ingesters, _, _ := prepare(t, testConfig)
	require.Len(t, distributors, 1)

	// Push should succeed - 2 of 3 zones is quorum
	res, err := distributors[0].Push(ctx, createRequest())
	require.NoError(t, err)
	assert.Equal(t, emptyResponse, res)

	// Verify both healthy zones received data (quorum of 2-of-3 requires both).
	zonesWithData := make(map[string]bool)
	for _, ing := range ingesters {
		if ing.timeseries != nil && len(ing.timeseries) > 0 {
			zonesWithData[ing.zone] = true
		}
	}
	assert.True(t, zonesWithData["zone-a"], "zone-a must have data (required for quorum)")
	assert.True(t, zonesWithData["zone-b"], "zone-b must have data (required for quorum)")

	// Verify zone-c (network-partitioned) stored no data — its Push always fails.
	assert.False(t, zonesWithData["zone-c"], "zone-c should have no stored data (its Push fails)")
}

// TestPartition_ScaleUp_NewPartitionBehavior verifies that when multiple partitions exist, the
// distributor routes writes across all of them based on series hash, not just a single partition.
func TestPartition_ScaleUp_NewPartitionBehavior(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	now := time.Now()
	mtime.NowForce(now)
	t.Cleanup(mtime.NowReset)

	// Push many series with distinct names to increase the probability of hitting
	// multiple partitions via DoBatch hash-based routing.
	ts := make([]mimirpb.PreallocTimeseries, 0, 20)
	for i := 0; i < 20; i++ {
		ts = append(ts, makeTimeseries(
			[]string{model.MetricNameLabel, fmt.Sprintf("scale_up_metric_%d", i)},
			makeSamples(now.UnixMilli(), float64(i)),
			nil, nil,
		))
	}
	req := &mimirpb.WriteRequest{Timeseries: ts}

	// Use 3 ingesters per zone so that ingester IDs 0, 1, 2 own partitions 0, 1, 2 respectively.
	testConfig := prepConfig{
		numDistributors:         1,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 3,
		ingesterStateByZone: map[string]ingesterZoneState{
			"zone-a": {numIngesters: 3, happyIngesters: 3},
			"zone-b": {numIngesters: 3, happyIngesters: 3},
			"zone-c": {numIngesters: 3, happyIngesters: 3},
		},
		ingesterIngestionType: ingesterIngestionTypeGRPC,
		limits:                prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.IngestStorageConfig.KafkaConfig.Enabled = false
			cfg.IngestStorageConfig.KafkaConfig.Address = ""
			cfg.IngestStorageConfig.Migration.WritePercentage = 100
		},
	}

	distributors, ingesters, _, _ := prepare(t, testConfig)
	require.Len(t, distributors, 1)

	res, err := distributors[0].Push(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, emptyResponse, res)

	// Verify that data was distributed across all 3 zones.
	zonesWithData := make(map[string]bool)
	for _, ing := range ingesters {
		if len(ing.timeseries) > 0 {
			zonesWithData[ing.zone] = true
		}
	}
	assert.Len(t, zonesWithData, 3, "all 3 zones should have received data")

	// Verify that multiple partitions received data across the ingesters.
	// Each ingester owns the partition matching its id (sequence number within zone).
	partitionsWithData := make(map[int]bool)
	for _, ing := range ingesters {
		if len(ing.timeseries) > 0 {
			partitionsWithData[ing.id] = true
		}
	}
	assert.Greater(t, len(partitionsWithData), 1, "multiple partitions should have received data from varied series")
}

// TestPartition_ScaleDown_InactivePartitionLookback verifies that when one partition is INACTIVE
// and one is ACTIVE, the distributor only routes writes to the ACTIVE partition and does not crash
// or error due to the presence of an INACTIVE partition.
func TestPartition_ScaleDown_InactivePartitionLookback(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	now := time.Now()
	mtime.NowForce(now)
	t.Cleanup(mtime.NowReset)

	createRequest := func() *mimirpb.WriteRequest {
		return &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				makeTimeseries([]string{model.MetricNameLabel, "series_one"}, makeSamples(now.UnixMilli(), 1), nil, nil),
			},
		}
	}

	// Use 2 ingesters per zone so that ingester IDs 0 and 1 own partitions 0 and 1 respectively.
	testConfig := prepConfig{
		numDistributors:         1,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 2, // partition 0 = INACTIVE, partition 1 = ACTIVE
		ingestStoragePartitionStates: map[int32]ring.PartitionState{
			0: ring.PartitionInactive,
			1: ring.PartitionActive,
		},
		ingesterStateByZone: map[string]ingesterZoneState{
			"zone-a": {numIngesters: 2, happyIngesters: 2},
			"zone-b": {numIngesters: 2, happyIngesters: 2},
			"zone-c": {numIngesters: 2, happyIngesters: 2},
		},
		ingesterIngestionType: ingesterIngestionTypeGRPC,
		limits:                prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.IngestStorageConfig.KafkaConfig.Enabled = false
			cfg.IngestStorageConfig.KafkaConfig.Address = ""
			cfg.IngestStorageConfig.Migration.WritePercentage = 100
		},
	}

	distributors, ingesters, _, _ := prepare(t, testConfig)
	require.Len(t, distributors, 1)

	// Push should succeed — the distributor should route to the ACTIVE partition only.
	res, err := distributors[0].Push(ctx, createRequest())
	require.NoError(t, err)
	assert.Equal(t, emptyResponse, res)

	// Verify that ingesters received data (the ACTIVE partition's owners got the write).
	totalSeries := 0
	for _, ing := range ingesters {
		totalSeries += len(ing.timeseries)
	}
	assert.Greater(t, totalSeries, 0, "ingesters should have received data via the active partition")

	// Verify the partition metrics reflect the correct state.
	distributors[0].updatePartitionMetrics()
	assert.Equal(t, float64(ring.PartitionInactive), testutil.ToFloat64(distributors[0].partitionState.WithLabelValues("0")),
		"partition 0 should be reported as INACTIVE")
	assert.Equal(t, float64(ring.PartitionActive), testutil.ToFloat64(distributors[0].partitionState.WithLabelValues("1")),
		"partition 1 should be reported as ACTIVE")
}

// TestDistributor_SendWriteRequestWithPercentageSplit verifies the mid-migration split path:
// when WritePercentage is between 0 and 100, series are split across classic and partition paths
// based on their hash.
func TestDistributor_SendWriteRequestWithPercentageSplit(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	now := time.Now()
	mtime.NowForce(now)
	t.Cleanup(mtime.NowReset)

	tests := map[string]struct {
		writePercentage   int
		numSeries         int
		expectClassic     bool // expect classic path counter > 0
		expectPartition   bool // expect partition path counter > 0
	}{
		// tokenForLabels with userID "user" and metric names "split_test_metric_N" produces
		// tokens with token%100 in range [6..31]. WritePercentage=25 forces a real split:
		//   partition (token%100 < 25): series with tokens 6-24
		//   classic  (token%100 >= 25): series with tokens 25-31
		// This is the only sub-case that exercises the parallel dispatch in sendWriteRequestWithPercentageSplit.
		"WritePercentage=25 splits traffic across both paths": {
			writePercentage: 25,
			numSeries:       20,
			expectClassic:   true,
			expectPartition: true,
		},
		// All tokens in [6..31] < 32, so all series go to partition via the shortcut path.
		// The shortcut calls sendWriteRequestToBackends directly, which does not increment writePathRequests.
		// We verify the push still succeeds and routes to the partition path.
		"WritePercentage=32 routes all to partition via shortcut": {
			writePercentage: 32,
			numSeries:       20,
			expectClassic:   false,
			expectPartition: false, // shortcut path, counter not incremented
		},
		// All tokens in [6..31] >= 5, so all series go to classic via the shortcut path.
		"WritePercentage=5 routes all to classic via shortcut": {
			writePercentage: 5,
			numSeries:       20,
			expectClassic:   false, // shortcut path, counter not incremented
			expectPartition: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ts := make([]mimirpb.PreallocTimeseries, 0, testData.numSeries)
			for i := 0; i < testData.numSeries; i++ {
				ts = append(ts, makeTimeseries(
					[]string{model.MetricNameLabel, fmt.Sprintf("split_test_metric_%d", i)},
					makeSamples(now.UnixMilli(), float64(i)),
					nil, nil,
				))
			}
			req := &mimirpb.WriteRequest{Timeseries: ts}

			testConfig := prepConfig{
				numDistributors:         1,
				ingestStorageEnabled:    true,
				ingestStoragePartitions: 1,
				ingesterStateByZone: map[string]ingesterZoneState{
					"zone-a": {numIngesters: 1, happyIngesters: 1},
					"zone-b": {numIngesters: 1, happyIngesters: 1},
					"zone-c": {numIngesters: 1, happyIngesters: 1},
				},
				ingesterIngestionType: ingesterIngestionTypeGRPC,
				limits:                prepareDefaultLimits(),
				configure: func(cfg *Config) {
					cfg.IngestStorageConfig.KafkaConfig.Enabled = false
					cfg.IngestStorageConfig.KafkaConfig.Address = ""
					cfg.IngestStorageConfig.Migration.WritePercentage = testData.writePercentage
				},
			}

			distributors, _, _, _ := prepare(t, testConfig)
			require.Len(t, distributors, 1)

			_, err := distributors[0].Push(ctx, req)
			require.NoError(t, err)

			classicCount := testutil.ToFloat64(distributors[0].writePathRequests.WithLabelValues("classic"))
			partitionCount := testutil.ToFloat64(distributors[0].writePathRequests.WithLabelValues("partition"))

			if testData.expectClassic {
				assert.Greater(t, classicCount, float64(0), "classic write path should have been used")
			}
			if testData.expectPartition {
				assert.Greater(t, partitionCount, float64(0), "partition write path should have been used")
			}
		})
	}
}

// TestDistributor_UpdatePartitionMetrics verifies that updatePartitionMetrics correctly reports
// partition states and healthy owner counts.
func TestDistributor_UpdatePartitionMetrics(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	_ = ctx

	// Use 2 ingesters per zone so that ingester IDs 0 and 1 own partitions 0 and 1 respectively.
	testConfig := prepConfig{
		numDistributors:         1,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 2,
		ingestStoragePartitionStates: map[int32]ring.PartitionState{
			0: ring.PartitionActive,
			1: ring.PartitionInactive,
		},
		ingesterStateByZone: map[string]ingesterZoneState{
			"zone-a": {numIngesters: 2, happyIngesters: 2},
			"zone-b": {numIngesters: 2, happyIngesters: 2},
			"zone-c": {numIngesters: 2, happyIngesters: 2},
		},
		ingesterIngestionType: ingesterIngestionTypeGRPC,
		limits:                prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.IngestStorageConfig.KafkaConfig.Enabled = false
			cfg.IngestStorageConfig.KafkaConfig.Address = ""
			cfg.IngestStorageConfig.Migration.WritePercentage = 100
		},
	}

	distributors, _, _, _ := prepare(t, testConfig)
	require.Len(t, distributors, 1)

	d := distributors[0]

	// Call updatePartitionMetrics and verify state metrics.
	d.updatePartitionMetrics()

	// Partition 0 is ACTIVE, partition 1 is INACTIVE.
	assert.Equal(t, float64(ring.PartitionActive), testutil.ToFloat64(d.partitionState.WithLabelValues("0")),
		"partition 0 should be reported as ACTIVE")
	assert.Equal(t, float64(ring.PartitionInactive), testutil.ToFloat64(d.partitionState.WithLabelValues("1")),
		"partition 1 should be reported as INACTIVE")

	// Both partitions have 3 healthy owners (one per zone, all ACTIVE in the ingester ring).
	assert.Equal(t, float64(3), testutil.ToFloat64(d.partitionHealthyOwners.WithLabelValues("0")),
		"partition 0 should have 3 healthy owners")
	assert.Equal(t, float64(3), testutil.ToFloat64(d.partitionHealthyOwners.WithLabelValues("1")),
		"partition 1 should have 3 healthy owners (owners are still healthy even if partition is inactive)")
}

// TestDistributor_BUG001_WritePercentageZeroRoutesToClassicWhenKafkaDisabled verifies that
// WritePercentage=0 with Kafka disabled routes all writes to the classic ingester ring.
// BUG-001: Currently partitionsSubring is set and ingestersSubring stays nil when WP=0
// and DistributorSendToIngestersEnabled=false, so sendWriteRequestToBackends routes
// everything to partitions instead of classic ingesters.
func TestDistributor_BUG001_WritePercentageZeroRoutesToClassicWhenKafkaDisabled(t *testing.T) {
	t.Parallel()

	ctx := user.InjectOrgID(context.Background(), "user")
	now := time.Now()

	testConfig := prepConfig{
		numDistributors:         1,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 1,
		ingesterStateByZone: map[string]ingesterZoneState{
			"zone-a": {numIngesters: 1, happyIngesters: 1},
			"zone-b": {numIngesters: 1, happyIngesters: 1},
			"zone-c": {numIngesters: 1, happyIngesters: 1},
		},
		ingesterIngestionType: ingesterIngestionTypeGRPC,
		limits:                prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.IngestStorageConfig.KafkaConfig.Enabled = false
			cfg.IngestStorageConfig.KafkaConfig.Address = ""
			cfg.IngestStorageConfig.Migration.WritePercentage = 0
			// Deliberately NOT setting DistributorSendToIngestersEnabled.
			// Per the design doc, WP=0 alone should mean "100% classic routing".
		},
	}

	distributors, _, _, _ := prepare(t, testConfig)
	require.Len(t, distributors, 1)

	d := distributors[0]

	_, err := d.Push(ctx, &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries([]string{model.MetricNameLabel, "bug001_series"}, makeSamples(now.UnixMilli(), 1), nil, nil),
		},
	})
	require.NoError(t, err)

	// WP=0 should route to classic path only.
	assert.Equal(t, float64(1), testutil.ToFloat64(d.writePathRequests.WithLabelValues("classic")),
		"WP=0 should increment classic counter")
	assert.Equal(t, float64(0), testutil.ToFloat64(d.writePathRequests.WithLabelValues("partition")),
		"WP=0 should NOT increment partition counter")
}

// errGetAllHealthyRing wraps a ReadRing and forces GetAllHealthy to return a fixed error.
// This simulates the ring returning ErrEmptyRing without needing to manipulate real ring state.
type errGetAllHealthyRing struct {
	ring.ReadRing
	err error
}

func (r *errGetAllHealthyRing) GetAllHealthy(_ ring.Operation) (ring.ReplicationSet, error) {
	return ring.ReplicationSet{}, r.err
}

// TestDistributor_BUG010_CleanupCalledWhenGetAllHealthyFails verifies that sendWriteRequestToPartitions
// calls batchOptions.Cleanup() even when GetAllHealthy returns an error before DoBatchWithOptions.
// BUG-010: the early return at line 2457 skips the DoBatchWithOptions call entirely, so
// batchOptions.Cleanup() (which releases the pooled write-request buffer) is never invoked.
// DoBatchWithOptions' own contract (batch.go:81) guarantees Cleanup is "always called" — but
// only if DoBatch actually runs. When we return before DoBatch, the caller must call Cleanup.
func TestDistributor_BUG010_CleanupCalledWhenGetAllHealthyFails(t *testing.T) {
	t.Parallel()

	testConfig := prepConfig{
		numDistributors:         1,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 1,
		ingesterStateByZone: map[string]ingesterZoneState{
			"zone-a": {numIngesters: 1, happyIngesters: 1},
			"zone-b": {numIngesters: 1, happyIngesters: 1},
			"zone-c": {numIngesters: 1, happyIngesters: 1},
		},
		ingesterIngestionType: ingesterIngestionTypeGRPC,
		limits:                prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.IngestStorageConfig.KafkaConfig.Enabled = false
			cfg.IngestStorageConfig.KafkaConfig.Address = ""
			cfg.IngestStorageConfig.Migration.WritePercentage = 100
		},
	}

	distributors, _, _, _ := prepare(t, testConfig)
	require.Len(t, distributors, 1)
	d := distributors[0]

	// Replace ingestersRing with a mock that always fails GetAllHealthy.
	// dskit's real ring only returns ErrEmptyRing when ringDesc is nil or has zero instances;
	// LEAVING ingesters still produce a non-error empty set.  This mock forces the error path.
	d.ingestersRing = &errGetAllHealthyRing{ReadRing: d.ingestersRing, err: ring.ErrEmptyRing}

	// Track whether batchOptions.Cleanup is invoked.
	cleanupCalled := atomic.NewInt64(0)
	batchOptions := ring.DoBatchOptions{
		Cleanup: func() { cleanupCalled.Inc() },
	}

	ctx := user.InjectOrgID(context.Background(), "user")
	// sendWriteRequestToPartitions returns before DoBatch when GetAllHealthy fails,
	// so tenantRing/req/keys are never touched — pass minimal values.
	err := d.sendWriteRequestToPartitions(
		ctx, "user",
		nil,                       // tenantRing — unused before the early return
		&mimirpb.WriteRequest{},   // req — unused before the early return
		nil,                       // keys — unused before the early return
		0,                         // initialMetadataIndex
		func() context.Context { return ctx },
		batchOptions,
	)
	require.Error(t, err, "should fail because GetAllHealthy returned ErrEmptyRing")

	// BUG-010: Currently Cleanup is NOT called on the GetAllHealthy-error path.
	// The fix must ensure Cleanup fires even when we return before DoBatchWithOptions.
	assert.Equal(t, int64(1), cleanupCalled.Load(),
		"batchOptions.Cleanup must be called even when GetAllHealthy fails before DoBatch")
}

// TestDistributor_BUG040_PartitionPushErrorPreservesIngesterCause verifies that when an
// ingester returns a gRPC error with a specific ErrorCause (e.g., INGESTION_RATE_LIMITED),
// the cause is preserved through wrapPartitionPushError and surfaces in the final gRPC response.
// BUG-040: wrapPartitionPushError wraps with cause=UNKNOWN, shadowing the inner
// ingesterPushError's cause. errors.As finds partitionPushError first → maps to codes.Internal (500).
func TestDistributor_BUG040_PartitionPushErrorPreservesIngesterCause(t *testing.T) {
	t.Parallel()

	ctx := user.InjectOrgID(context.Background(), "user")
	now := time.Now()

	testConfig := prepConfig{
		numDistributors:         1,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 1,
		ingesterStateByZone: map[string]ingesterZoneState{
			"zone-a": {numIngesters: 1, happyIngesters: 1},
			"zone-b": {numIngesters: 1, happyIngesters: 1},
			"zone-c": {numIngesters: 1, happyIngesters: 1},
		},
		ingesterIngestionType: ingesterIngestionTypeGRPC,
		limits:                prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.IngestStorageConfig.KafkaConfig.Enabled = false
			cfg.IngestStorageConfig.KafkaConfig.Address = ""
			cfg.IngestStorageConfig.Migration.WritePercentage = 100
		},
	}

	distributors, ingesters, _, _ := prepare(t, testConfig)
	require.Len(t, distributors, 1)

	// Make ALL ingesters return a rate-limited error with proper gRPC ErrorDetails.
	rateLimitedErr := createStatusWithDetails(t, codes.ResourceExhausted, "rate limited", mimirpb.ERROR_CAUSE_INGESTION_RATE_LIMITED).Err()
	for _, ing := range ingesters {
		ing.registerBeforePushHook(func(_ context.Context, _ *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error, bool) {
			return nil, rateLimitedErr, true
		})
	}

	_, err := distributors[0].Push(ctx, &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries([]string{model.MetricNameLabel, "bug040_series"}, makeSamples(now.UnixMilli(), 1), nil, nil),
		},
	})
	require.Error(t, err)

	// The error should preserve the INGESTION_RATE_LIMITED cause and map to ResourceExhausted (429).
	// BUG-040: Currently wrapPartitionPushError sets cause=UNKNOWN, so errors.As finds
	// partitionPushError first and maps to codes.Internal (500).
	stat, ok := grpcutil.ErrorToStatus(err)
	require.True(t, ok, "error should be a gRPC status error")

	assert.Equal(t, codes.ResourceExhausted, stat.Code(),
		"rate-limited ingester error should surface as ResourceExhausted (429), not Internal (500)")

	details := stat.Details()
	require.Len(t, details, 1, "should have ErrorDetails")
	errorDetails, ok := details[0].(*mimirpb.ErrorDetails)
	require.True(t, ok)
	assert.Equal(t, mimirpb.ERROR_CAUSE_INGESTION_RATE_LIMITED, errorDetails.Cause,
		"error cause should be INGESTION_RATE_LIMITED, not UNKNOWN")
}

// TestDistributor_BUG011_UpdatePartitionMetrics_UsesRingHeartbeatTimeout verifies that
// updatePartitionMetrics uses the ring's HeartbeatTimeout (not PoolConfig.RemoteTimeout)
// to determine ingester health, matching the write path's health check.
// merged_bug_011: updatePartitionMetrics calls instance.IsHealthy(ring.Write,
// d.cfg.PoolConfig.RemoteTimeout, now) but the write path uses d.ingestersRing.GetAllHealthy
// which uses the ring's HeartbeatTimeout (default 1 min). With RemoteTimeout=2s, ingesters
// appear unhealthy in metrics even when perfectly healthy for writes.
func TestDistributor_BUG011_UpdatePartitionMetrics_UsesRingHeartbeatTimeout(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	_ = ctx

	testConfig := prepConfig{
		numDistributors:         1,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 1,
		ingesterStateByZone: map[string]ingesterZoneState{
			"zone-a": {numIngesters: 1, happyIngesters: 1},
			"zone-b": {numIngesters: 1, happyIngesters: 1},
			"zone-c": {numIngesters: 1, happyIngesters: 1},
		},
		ingesterIngestionType: ingesterIngestionTypeGRPC,
		limits:                prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.IngestStorageConfig.KafkaConfig.Enabled = false
			cfg.IngestStorageConfig.KafkaConfig.Address = ""
			cfg.IngestStorageConfig.Migration.WritePercentage = 100
			// Set the top-level RemoteTimeout to 1ns.  New() copies this into
			// cfg.PoolConfig.RemoteTimeout (distributor.go:484), which is the value
			// updatePartitionMetrics passes to IsHealthy.  Any ingester whose heartbeat
			// is older than 1ns will appear unhealthy.  The ring's own HeartbeatTimeout
			// is ~1 min, so ingesters SHOULD appear healthy for writes — the metric is wrong.
			cfg.RemoteTimeout = time.Nanosecond
		},
	}

	distributors, _, _, _ := prepare(t, testConfig)
	require.Len(t, distributors, 1)

	d := distributors[0]
	d.updatePartitionMetrics()

	// All 3 ingesters are healthy (heartbeat just set by prepare). With the ring's HeartbeatTimeout
	// (~1 min), all are healthy. With RemoteTimeout (1ns), all appear unhealthy.
	// BUG-011: updatePartitionMetrics uses RemoteTimeout, so this will be 0 instead of 3.
	assert.Equal(t, float64(3), testutil.ToFloat64(d.partitionHealthyOwners.WithLabelValues("0")),
		"all partition owners should be healthy (ring heartbeat timeout is ~1min, not RemoteTimeout)")
}

// TestDistributor_PartitionRingWithoutKafka_MigrationQueryContinuity verifies that a series written
// first via the classic ingester ring (WritePercentage=0) and then via partition owners
// (WritePercentage=100) can be queried back as a single continuous series through QueryStream.
//
// Classic and partition writes use different ring lookups, so they land on different 2-of-3 zone
// write quorums. The quorum intersection property guarantees that any 2-zone read quorum overlaps
// with both write quorums, so all samples are visible regardless of which zones the read hits.
func TestDistributor_PartitionRingWithoutKafka_MigrationQueryContinuity(t *testing.T) {
	const (
		numSeries = 5
		orgID     = "test"
	)

	t0 := int64(1_000_000) // timestamps in ms
	t1 := t0 + 60_000      // +1 minute

	distributors, _, reg, _ := prepare(t, prepConfig{
		numDistributors: 1,
		ingesterStateByZone: map[string]ingesterZoneState{
			"zone-a": {numIngesters: 1, happyIngesters: 1},
			"zone-b": {numIngesters: 1, happyIngesters: 1},
			"zone-c": {numIngesters: 1, happyIngesters: 1},
		},
		ingesterIngestionType:   ingesterIngestionTypeGRPC,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 1,
		limits:                  prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.IngestStorageConfig.KafkaConfig.Enabled = false
			cfg.IngestStorageConfig.KafkaConfig.Address = ""
			cfg.IngestStorageConfig.Migration.WritePercentage = 0
		},
	})
	require.Len(t, distributors, 1)
	d := distributors[0]

	ctx := user.InjectOrgID(context.Background(), orgID)

	// Push at t0 via classic path (WP=0).
	req0 := &mimirpb.WriteRequest{}
	for i := 0; i < numSeries; i++ {
		req0.Timeseries = append(req0.Timeseries, makeTimeseries(
			[]string{model.MetricNameLabel, fmt.Sprintf("migration_test_%d", i)},
			makeSamples(t0, float64(i)),
			nil, nil,
		))
	}
	_, err := d.Push(ctx, req0)
	require.NoError(t, err)

	// Switch to partition routing (WP=100) and push at t1.
	d.cfg.IngestStorageConfig.Migration.WritePercentage = 100
	req1 := &mimirpb.WriteRequest{}
	for i := 0; i < numSeries; i++ {
		req1.Timeseries = append(req1.Timeseries, makeTimeseries(
			[]string{model.MetricNameLabel, fmt.Sprintf("migration_test_%d", i)},
			makeSamples(t1, float64(i)+100),
			nil, nil,
		))
	}
	_, err = d.Push(ctx, req1)
	require.NoError(t, err)
	// Brief pause: the partition-owner write path orphans a background goroutine
	// (ReplicationSet.Do returns after 2-of-3 zone quorum). That goroutine must
	// finish marshaling the request before the next push can reclaim the buffer
	// from the pool. 1 ms is plenty for the instant mock.
	time.Sleep(time.Millisecond)

	// QueryStream fans out to a 2-of-3 zone read quorum.  The quorum
	// intersection property guarantees that any sample written to 2 zones is
	// visible from any 2-zone read — the classic sample (t0) and the partition
	// sample (t1) are both returned even though they were written via different
	// ring lookups that may have selected different zone subsets.
	queryCtx := limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
	queryCtx = api.ContextWithReadConsistencyLevel(queryCtx, api.ReadConsistencyStrong)
	queryMetrics := stats.NewQueryMetrics(reg[0])
	matchers := labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, "migration_test_.*")

	res, err := d.QueryStream(queryCtx, queryMetrics, model.Time(t0), model.Time(t1), matchers)
	require.NoError(t, err)
	require.Len(t, res.StreamingSeries, numSeries, "expected %d series back from QueryStream", numSeries)

	for _, series := range res.StreamingSeries {
		allSamples := collectSamplesFromSources(t, series, model.Time(t0), model.Time(t1))
		require.Len(t, allSamples, 2, "series %s: expected 2 samples, got %d", series.Labels, len(allSamples))
		assert.Equal(t, model.Time(t0), allSamples[0].Timestamp, "series %s: first sample should be at t0", series.Labels)
		assert.Equal(t, model.Time(t1), allSamples[1].Timestamp, "series %s: second sample should be at t1", series.Labels)
	}
}

// TestDistributor_PartitionRingWithoutKafka_FlipFlopQueryContinuity verifies that a series whose
// samples alternate between classic routing (WP=0) and partition routing (WP=100) — as happens
// during a gradual distributor rollout — can still be queried back as a single continuous series.
//
// Classic and partition writes land on different 2-of-3 zone quorums, but the quorum intersection
// property guarantees that any 2-zone read quorum overlaps with every write quorum, so all samples
// are visible. Sources ≥ 2 proves QueryStream actually fans out to multiple ingesters.
func TestDistributor_PartitionRingWithoutKafka_FlipFlopQueryContinuity(t *testing.T) {
	const (
		numSeries = 5
		orgID     = "test"
	)

	t0 := int64(1_000_000)
	t1 := t0 + 60_000
	t2 := t1 + 60_000
	t3 := t2 + 60_000

	distributors, _, reg, _ := prepare(t, prepConfig{
		numDistributors: 1,
		ingesterStateByZone: map[string]ingesterZoneState{
			"zone-a": {numIngesters: 1, happyIngesters: 1},
			"zone-b": {numIngesters: 1, happyIngesters: 1},
			"zone-c": {numIngesters: 1, happyIngesters: 1},
		},
		ingesterIngestionType:   ingesterIngestionTypeGRPC,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 1,
		limits:                  prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.IngestStorageConfig.KafkaConfig.Enabled = false
			cfg.IngestStorageConfig.KafkaConfig.Address = ""
			cfg.IngestStorageConfig.Migration.WritePercentage = 0
		},
	})
	require.Len(t, distributors, 1)
	d := distributors[0]

	ctx := user.InjectOrgID(context.Background(), orgID)

	pushAt := func(ts int64, value float64) {
		req := &mimirpb.WriteRequest{}
		for i := 0; i < numSeries; i++ {
			req.Timeseries = append(req.Timeseries, makeTimeseries(
				[]string{model.MetricNameLabel, fmt.Sprintf("flipflop_test_%d", i)},
				makeSamples(ts, value+float64(i)),
				nil, nil,
			))
		}
		_, err := d.Push(ctx, req)
		require.NoError(t, err)
		// Brief pause: the partition-owner write path orphans a background goroutine
		// (ReplicationSet.Do returns after 2-of-3 zone quorum).  That goroutine must
		// finish marshaling the request before the next push can reclaim the buffer
		// from the pool.  1 ms is plenty for the instant mock.
		time.Sleep(time.Millisecond)
	}

	// Push 4 rounds alternating classic ↔ partition routing to simulate a gradual
	// rollout where WritePercentage flips between distributors.
	d.cfg.IngestStorageConfig.Migration.WritePercentage = 0   // classic
	pushAt(t0, 0)
	d.cfg.IngestStorageConfig.Migration.WritePercentage = 100 // partition
	pushAt(t1, 100)
	d.cfg.IngestStorageConfig.Migration.WritePercentage = 0   // classic
	pushAt(t2, 200)
	d.cfg.IngestStorageConfig.Migration.WritePercentage = 100 // partition
	pushAt(t3, 300)

	// QueryStream fans out to a 2-of-3 zone read quorum.  Each push wrote to
	// a 2-zone write quorum (which may differ between classic and partition
	// ring lookups).  The quorum intersection property guarantees every sample
	// is visible from any 2-zone read.
	queryCtx := limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
	queryCtx = api.ContextWithReadConsistencyLevel(queryCtx, api.ReadConsistencyStrong)
	queryMetrics := stats.NewQueryMetrics(reg[0])
	matchers := labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, "flipflop_test_.*")

	res, err := d.QueryStream(queryCtx, queryMetrics, model.Time(t0), model.Time(t3), matchers)
	require.NoError(t, err)
	require.Len(t, res.StreamingSeries, numSeries, "expected %d series back from QueryStream", numSeries)

	for _, series := range res.StreamingSeries {
		allSamples := collectSamplesFromSources(t, series, model.Time(t0), model.Time(t3))
		require.Len(t, allSamples, 4, "series %s: expected 4 samples, got %d", series.Labels, len(allSamples))
		assert.Equal(t, model.Time(t0), allSamples[0].Timestamp, "series %s: sample 0 should be t0", series.Labels)
		assert.Equal(t, model.Time(t1), allSamples[1].Timestamp, "series %s: sample 1 should be t1", series.Labels)
		assert.Equal(t, model.Time(t2), allSamples[2].Timestamp, "series %s: sample 2 should be t2", series.Labels)
		assert.Equal(t, model.Time(t3), allSamples[3].Timestamp, "series %s: sample 3 should be t3", series.Labels)
	}
}

// collectSamplesFromSources decodes all float samples from a StreamingSeries across all source
// ingesters, deduplicates by timestamp (overlapping zones in a quorum may both have a sample),
// and returns them sorted by timestamp.
func collectSamplesFromSources(t *testing.T, series client.StreamingSeries, from, through model.Time) []model.SamplePair {
	t.Helper()
	seen := map[model.Time]struct{}{}
	var allSamples []model.SamplePair
	for _, source := range series.Sources {
		wireChunks, err := source.StreamReader.GetChunks(source.SeriesIndex)
		require.NoError(t, err)

		chunks, err := client.FromChunks(series.Labels, wireChunks)
		require.NoError(t, err)

		for i := range chunks {
			samples, _, err := chunks[i].Samples(from, through)
			require.NoError(t, err)
			for _, s := range samples {
				if _, ok := seen[s.Timestamp]; !ok {
					seen[s.Timestamp] = struct{}{}
					allSamples = append(allSamples, s)
				}
			}
		}
	}
	sort.Slice(allSamples, func(i, j int) bool {
		return allSamples[i].Timestamp < allSamples[j].Timestamp
	})
	return allSamples
}
