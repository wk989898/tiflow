// Copyright 2020 PingCAP, Inc.
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

package main

import (
	"context"
	"strings"
	"time"

	"github.com/IBM/sarama"
	cerror "github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
)

func newConfig(o *ConsumerOption) (*sarama.Config, error) {
	config := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(o.version)
	if err != nil {
		return nil, cerror.Trace(err)
	}

	config.ClientID = "ticdc_kafka_sarama_consumer"
	config.Version = version

	config.Metadata.Retry.Max = 10000
	config.Metadata.Retry.Backoff = 500 * time.Millisecond
	config.Consumer.Retry.Backoff = 500 * time.Millisecond
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	if len(o.ca) != 0 {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config, err = (&security.Credential{
			CAPath:   o.ca,
			CertPath: o.cert,
			KeyPath:  o.key,
		}).ToTLSConfig()
		if err != nil {
			return nil, cerror.Trace(err)
		}
	}

	return config, err
}

func saramWaitTopicCreated(address []string, topic string, cfg *sarama.Config) error {
	admin, err := sarama.NewClusterAdmin(address, cfg)
	if err != nil {
		return cerror.Trace(err)
	}
	defer admin.Close()
	for i := 0; i <= 30; i++ {
		topics, err := admin.ListTopics()
		if err != nil {
			return cerror.Trace(err)
		}
		if _, ok := topics[topic]; ok {
			return nil
		}
		log.Info("wait the topic created", zap.String("topic", topic))
		time.Sleep(1 * time.Second)
	}
	return cerror.Errorf("wait the topic(%s) created timeout", topic)
}

func saramGetPartitionNum(address []string, topic string) (int32, error) {
	saramaConfig := sarama.NewConfig()
	// get partition number or create topic automatically
	admin, err := sarama.NewClusterAdmin(address, saramaConfig)
	if err != nil {
		return 0, cerror.Trace(err)
	}
	topics, err := admin.ListTopics()
	if err != nil {
		return 0, cerror.Trace(err)
	}
	err = admin.Close()
	if err != nil {
		return 0, cerror.Trace(err)
	}
	topicDetail, exist := topics[topic]
	if !exist {
		return 0, cerror.Errorf("can not find topic %s", topic)
	}
	log.Info("get partition number of topic",
		zap.String("topic", topic),
		zap.Int32("partitionNum", topicDetail.NumPartitions))
	return topicDetail.NumPartitions, nil
}

type SaramConsumer struct {
	ready  chan bool
	option *ConsumerOption
	config *sarama.Config
	writer *Writer
}

var _ KakfaConsumer = (*SaramConsumer)(nil)

func NewSaramConsumer(ctx context.Context, o *ConsumerOption) KakfaConsumer {
	c := new(SaramConsumer)
	w, err := NewWriter(ctx, o)
	if err != nil {
		log.Panic("Error creating writer", zap.Error(err))
	}
	c.writer = w
	if o.partitionNum != 0 {
		partitionNum, err := saramGetPartitionNum(o.address, o.topic)
		if err != nil {
			log.Panic("can not get partition number", zap.String("topic", o.topic), zap.Error(err))
		}
		o.partitionNum = partitionNum
	}
	c.option = o

	c.ready = make(chan bool)
	config, err := newConfig(o)

	if err != nil {
		log.Panic("Error creating sarama config", zap.Error(err))
	}
	c.config = config
	err = saramWaitTopicCreated(o.address, o.topic, config)
	// wait topic create
	if err != nil {
		log.Panic("wait topic created failed", zap.Error(err))
	}
	return c
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *SaramConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the c as ready
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *SaramConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// async read message from Kafka
func (c *SaramConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	partition := claim.Partition()
	log.Info("start consume claim",
		zap.String("topic", claim.Topic()), zap.Int32("partition", partition),
		zap.Int64("initialOffset", claim.InitialOffset()), zap.Int64("highWaterMarkOffset", claim.HighWaterMarkOffset()))
	eventGroups := make(map[int64]*EventsGroup)
	for message := range claim.Messages() {
		if err := c.writer.Decode(context.Background(), c.option, partition, message.Key, message.Value, eventGroups); err != nil {
			return err
		}
		// sync write to downstream
		if err := c.writer.Write(context.Background()); err != nil {
			log.Panic("Error write to downstream", zap.Error(err))
		}
		session.MarkMessage(message, "")
	}
	return nil
}

func (c *SaramConsumer) Consume(ctx context.Context) error {
	client, err := sarama.NewConsumerGroup(c.option.address, c.option.groupID, c.config)
	if err != nil {
		log.Panic("Error creating consumer group client", zap.Error(err))
	}

	defer func() {
		if err = client.Close(); err != nil {
			log.Panic("Error closing client", zap.Error(err))
		}
	}()

	for {
		// `consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := client.Consume(ctx, strings.Split(c.option.topic, ","), c); err != nil {
			log.Panic("Error from consumer", zap.Error(err))
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.ready = make(chan bool)
	}
}

// async write to downsteam
func (c *SaramConsumer) AsyncWrite(ctx context.Context) {
	c.writer.AsyncWrite(ctx)
}
