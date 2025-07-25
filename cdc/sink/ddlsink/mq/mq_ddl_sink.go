// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mq

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/mq/ddlproducer"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/manager"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"go.uber.org/zap"
)

// DDLDispatchRule is the dispatch rule for DDL event.
type DDLDispatchRule int

const (
	// PartitionZero means the DDL event will be dispatched to partition 0.
	// NOTICE: Only for canal and canal-json protocol.
	PartitionZero DDLDispatchRule = iota
	// PartitionAll means the DDL event will be broadcast to all the partitions.
	PartitionAll
)

func getDDLDispatchRule(protocol config.Protocol) DDLDispatchRule {
	switch protocol {
	case config.ProtocolCanal, config.ProtocolCanalJSON:
		return PartitionZero
	default:
	}
	return PartitionAll
}

// Assert Sink implementation
var _ ddlsink.Sink = (*DDLSink)(nil)

// DDLSink is a sink that sends DDL events to the MQ system.
type DDLSink struct {
	// id indicates which processor (changefeed) this sink belongs to.
	id model.ChangeFeedID
	// protocol indicates the protocol used by this sink.
	protocol config.Protocol
	// eventRouter used to route events to the right topic and partition.
	eventRouter *dispatcher.EventRouter
	// topicManager used to manage topics.
	// It is also responsible for creating topics.
	topicManager manager.TopicManager
	encoder      codec.RowEventEncoder
	// producer used to send events to the MQ system.
	// Usually it is a sync producer.
	producer ddlproducer.DDLProducer
	// statistics is used to record DDL metrics.
	statistics *metrics.Statistics
	// admin is used to query kafka cluster information.
	admin kafka.ClusterAdminClient
	// connRefresherForDDL is used to refresh the connection for DDL events.
	connRefresherForDDL kafka.SyncProducer
}

func newDDLSink(
	changefeedID model.ChangeFeedID,
	producer ddlproducer.DDLProducer,
	adminClient kafka.ClusterAdminClient,
	topicManager manager.TopicManager,
	eventRouter *dispatcher.EventRouter,
	encoder codec.RowEventEncoder,
	protocol config.Protocol,
	connRefresherForDDL kafka.SyncProducer,
) *DDLSink {
	return &DDLSink{
		id:                  changefeedID,
		protocol:            protocol,
		eventRouter:         eventRouter,
		topicManager:        topicManager,
		encoder:             encoder,
		producer:            producer,
		statistics:          metrics.NewStatistics(changefeedID, sink.RowSink),
		admin:               adminClient,
		connRefresherForDDL: connRefresherForDDL,
	}
}

// WriteDDLEvent encodes the DDL event and sends it to the MQ system.
func (k *DDLSink) WriteDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	msg, err := k.encoder.EncodeDDLEvent(ddl)
	if err != nil {
		return err
	}
	if msg == nil {
		log.Info("Skip ddl event", zap.Uint64("commitTs", ddl.CommitTs),
			zap.String("query", ddl.Query),
			zap.String("protocol", k.protocol.String()),
			zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID))
		return nil
	}

	topic := k.eventRouter.GetTopicForDDL(ddl)
	partitionRule := getDDLDispatchRule(k.protocol)
	log.Debug("Emit ddl event",
		zap.Uint64("commitTs", ddl.CommitTs),
		zap.String("query", ddl.Query),
		zap.String("namespace", k.id.Namespace),
		zap.String("changefeed", k.id.ID))
	// Notice: We must call GetPartitionNum here,
	// which will be responsible for automatically creating topics when they don't exist.
	// If it is not called here and kafka has `auto.create.topics.enable` turned on,
	// then the auto-created topic will not be created as configured by ticdc.
	partitionNum, err := k.topicManager.GetPartitionNum(ctx, topic)
	if err != nil {
		return err
	}

	if partitionRule == PartitionAll {
		return k.statistics.RecordDDLExecution(func() error {
			return k.producer.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
		})
	}
	return k.statistics.RecordDDLExecution(func() error {
		return k.producer.SyncSendMessage(ctx, topic, 0, msg)
	})
}

// WriteCheckpointTs sends the checkpoint ts to the MQ system.
// This function will be called at least once per second.
func (k *DDLSink) WriteCheckpointTs(ctx context.Context,
	ts uint64, tables []*model.TableInfo,
) error {
	// This operation is used to keep the kafka connection alive.
	// For more details, see https://github.com/pingcap/tiflow/pull/12173
	if k.connRefresherForDDL != nil {
		// The implementation is saramaSyncProducer.HeartbeatBrokers. And
		// there is a keepConnAliveInterval in the saramaSyncProducer, so
		// we don't need to worry about the heartbeat is too frequent.
		k.connRefresherForDDL.HeartbeatBrokers()
	}

	var (
		err          error
		partitionNum int32
	)
	msg, err := k.encoder.EncodeCheckpointEvent(ts)
	if err != nil {
		return err
	}
	if msg == nil {
		return nil
	}
	// NOTICE: When there are no tables to replicate,
	// we need to send checkpoint ts to the default topic.
	// This will be compatible with the old behavior.
	if len(tables) == 0 {
		topic := k.eventRouter.GetDefaultTopic()
		partitionNum, err = k.topicManager.GetPartitionNum(ctx, topic)
		if err != nil {
			return err
		}
		log.Debug("Emit checkpointTs to default topic",
			zap.String("topic", topic), zap.Uint64("checkpointTs", ts))
		return k.producer.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
	}
	var tableNames []model.TableName
	for _, table := range tables {
		tableNames = append(tableNames, table.TableName)
	}
	topics := k.eventRouter.GetActiveTopics(tableNames)
	for _, topic := range topics {
		partitionNum, err = k.topicManager.GetPartitionNum(ctx, topic)
		if err != nil {
			return err
		}
		err = k.producer.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes the sink.
func (k *DDLSink) Close() {
	if k.producer != nil {
		k.producer.Close()
	}
	if k.topicManager != nil {
		k.topicManager.Close()
	}
	if k.admin != nil {
		k.admin.Close()
	}
}
