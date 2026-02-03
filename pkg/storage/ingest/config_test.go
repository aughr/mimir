// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		setup       func(*Config)
		expectedErr error
	}{
		"should pass with the default config": {
			setup: func(_ *Config) {},
		},
		"should fail if ingest storage is enabled and Kafka address is not configured": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Topic = "test"
			},
			expectedErr: ErrMissingKafkaAddress,
		},
		"should fail if ingest storage is enabled and Kafka topic is not configured": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
			},
			expectedErr: ErrMissingKafkaTopic,
		},
		"should pass if ingest storage is enabled and required config is set": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
			},
		},
		"should fail if ingest storage is enabled and consume position is invalid": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ConsumeFromPositionAtStartup = "middle"
			},
			expectedErr: ErrInvalidConsumePosition,
		},
		"should fail if ingest storage is enabled and consume timestamp is set and consume position is not expected": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ConsumeFromPositionAtStartup = consumeFromEnd
				cfg.KafkaConfig.ConsumeFromTimestampAtStartup = time.Now().UnixMilli()
			},
			expectedErr: ErrInvalidConsumePosition,
		},
		"should fail if ingest storage is enabled and consume position is expected but consume timestamp is invalid": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ConsumeFromPositionAtStartup = consumeFromTimestamp
				cfg.KafkaConfig.ConsumeFromTimestampAtStartup = 0
			},
			expectedErr: ErrInvalidConsumePosition,
		},
		"should fail if ingest storage is enabled and the configured number of Kafka write clients is 0": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.WriteClients = 0
			},
			expectedErr: ErrInvalidWriteClients,
		},
		"should fail if ingest storage is enabled and producer max record size bytes is set too low": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ProducerMaxRecordSizeBytes = minProducerRecordDataBytesLimit - 1
			},
			expectedErr: ErrInvalidProducerMaxRecordSizeBytes,
		},
		"should fail if ingest storage is enabled and producer max record size bytes is set too high": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ProducerMaxRecordSizeBytes = maxProducerRecordDataBytesLimit + 1
			},
			expectedErr: ErrInvalidProducerMaxRecordSizeBytes,
		},
		"should fail if target consumer lag is enabled but max consumer lag is not": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.TargetConsumerLagAtStartup = 2 * time.Second
				cfg.KafkaConfig.MaxConsumerLagAtStartup = 0
			},
			expectedErr: ErrInconsistentConsumerLagAtStartup,
		},
		"should fail if max consumer lag is enabled but target consumer lag is not": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.TargetConsumerLagAtStartup = 0
				cfg.KafkaConfig.MaxConsumerLagAtStartup = 2 * time.Second
			},
			expectedErr: ErrInconsistentConsumerLagAtStartup,
		},
		"should fail if target consumer lag is > max consumer lag": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.TargetConsumerLagAtStartup = 2 * time.Second
				cfg.KafkaConfig.MaxConsumerLagAtStartup = 1 * time.Second
			},
			expectedErr: ErrInvalidMaxConsumerLagAtStartup,
		},
		"should fail if SASL username is configured but password is not": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASLUsername = "mimir"
			},
			expectedErr: ErrInconsistentSASLCredentials,
		},
		"should fail if SASL password is configured but username is not": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				require.NoError(t, cfg.KafkaConfig.SASLPassword.Set("supersecret"))
			},
			expectedErr: ErrInconsistentSASLCredentials,
		},
		"should pass if both SASL username and password are configured": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASLUsername = "mimir"
				require.NoError(t, cfg.KafkaConfig.SASLPassword.Set("supersecret"))
			},
		},
		"should fail if max ingestion concurrency is lower than 0": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.IngestionConcurrencyMax = -1
			},
			expectedErr: ErrInvalidIngestionConcurrencyMax,
		},
		"should pass if max ingestion concurrency is 0": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.IngestionConcurrencyMax = 0
			},
		},
		"should fail if ingestion concurrency batch size is lower than 0": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.IngestionConcurrencyMax = 5
				cfg.KafkaConfig.IngestionConcurrencyBatchSize = -1
			},
			expectedErr: ErrInvalidIngestionConcurrencyParams,
		},
		"should fail if ingestion concurrency queue capacity is lower than 0": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.IngestionConcurrencyMax = 5
				cfg.KafkaConfig.IngestionConcurrencyQueueCapacity = -1
			},
			expectedErr: ErrInvalidIngestionConcurrencyParams,
		},
		"should fail if ingestion concurrency estimates bytes per sample is lower than 0": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.IngestionConcurrencyMax = 5
				cfg.KafkaConfig.IngestionConcurrencyEstimatedBytesPerSample = -1
			},
			expectedErr: ErrInvalidIngestionConcurrencyParams,
		},
		"should fail if ingestion concurrency target flushes per shard is lower than 0": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.IngestionConcurrencyMax = 5
				cfg.KafkaConfig.IngestionConcurrencyTargetFlushesPerShard = -1
			},
			expectedErr: ErrInvalidIngestionConcurrencyParams,
		},
		"should fail when auto create topic default partitions is lower than 1": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.AutoCreateTopicEnabled = true
				cfg.KafkaConfig.AutoCreateTopicDefaultPartitions = -100
			},
			expectedErr: ErrInvalidAutoCreateTopicParams,
		},
		"should pass when auto create topic default partitions is -1 (using Kafka broker's default)": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.AutoCreateTopicEnabled = true
				cfg.KafkaConfig.AutoCreateTopicDefaultPartitions = -1
			},
		},
		"should fail if Kafka fetch max wait is less than 5s": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.FetchMaxWait = 2 * time.Second
			},
			expectedErr: ErrInvalidFetchMaxWait,
		},
		"should fail if Kafka fetch max wait is greater than 30s": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.FetchMaxWait = 32 * time.Second
			},
			expectedErr: ErrInvalidFetchMaxWait,
		},
		"should fail if fsync concurrency is not at least 1": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.WriteLogsFsyncBeforeKafkaCommitConcurrency = 0
			},

			expectedErr: ErrInvalidWriteLogsFsyncConcurrency,
		},
		"should fail if write percentage is negative": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.Migration.WritePercentage = -1
			},
			expectedErr: ErrInvalidWritePercentage,
		},
		"should fail if write percentage is greater than 100": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.Migration.WritePercentage = 101
			},
			expectedErr: ErrInvalidWritePercentage,
		},
		"should pass if write percentage is 0": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.Migration.WritePercentage = 0
			},
		},
		"should fail if write percentage is between 1-99 and kafka is enabled": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.Migration.WritePercentage = 50
			},
			expectedErr: ErrWritePercentageSplitWithKafkaEnabled,
		},
		"should pass if write percentage is 50 and kafka is disabled": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Enabled = false
				cfg.Migration.WritePercentage = 50
			},
		},
		"should pass if write percentage is 100": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.Migration.WritePercentage = 100
			},
		},
		"should fail if partition isolation enabled but write percentage is less than 100": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.PartitionIsolationEnabled = true
				cfg.Migration.WritePercentage = 50
			},
			expectedErr: ErrPartitionIsolationRequiresFull,
		},
		"should pass if partition isolation enabled and write percentage is 100": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.PartitionIsolationEnabled = true
				cfg.Migration.WritePercentage = 100
			},
		},
		"should fail if distributor_send_to_ingesters_enabled is set without kafka": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Enabled = false
				cfg.Migration.DistributorSendToIngestersEnabled = true
				cfg.Migration.WritePercentage = 100
			},
			expectedErr: ErrDistributorSendToIngestersWithoutKafka,
		},
		"should pass if distributor_send_to_ingesters_enabled is set with kafka": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.Migration.DistributorSendToIngestersEnabled = true
				cfg.Migration.WritePercentage = 0
			},
		},
		"should fail if kafka is disabled but address is configured": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Enabled = false
				cfg.KafkaConfig.Address = "localhost"
			},
			expectedErr: ErrKafkaAddressWithKafkaDisabled,
		},
		"should pass if kafka is disabled and address is empty": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Enabled = false
				cfg.KafkaConfig.Address = ""
			},
		},
		"should pass if kafka is disabled with partition isolation and write percentage 100": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Enabled = false
				cfg.KafkaConfig.Address = ""
				cfg.PartitionIsolationEnabled = true
				cfg.Migration.WritePercentage = 100
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := Config{}
			flagext.DefaultValues(&cfg)
			testData.setup(&cfg)

			assert.ErrorIs(t, cfg.Validate(), testData.expectedErr)
		})
	}
}

func TestConfig_GetConsumerGroup(t *testing.T) {
	tests := map[string]struct {
		consumerGroup string
		instanceID    string
		partitionID   int32
		expected      string
	}{
		"should return the instance ID if no consumer group is explicitly configured": {
			consumerGroup: "",
			instanceID:    "ingester-zone-a-1",
			partitionID:   1,
			expected:      "ingester-zone-a-1",
		},
		"should return the configured consumer group if set": {
			consumerGroup: "ingester-a",
			instanceID:    "ingester-zone-a-1",
			partitionID:   1,
			expected:      "ingester-a",
		},
		"should support <partition> placeholder in the consumer group": {
			consumerGroup: "ingester-zone-a-partition-<partition>",
			instanceID:    "ingester-zone-a-1",
			partitionID:   1,
			expected:      "ingester-zone-a-partition-1",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := KafkaConfig{ConsumerGroup: testData.consumerGroup}
			assert.Equal(t, testData.expected, cfg.GetConsumerGroup(testData.instanceID, testData.partitionID))
		})
	}
}
