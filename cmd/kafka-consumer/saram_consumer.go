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
	"sync"
	"time"

	"github.com/IBM/sarama"
	cerror "github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
)

func newConsumerGroup(consumerOption *consumerOption, config *sarama.Config) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(consumerOption.address, consumerOption.groupID, config)
}

func newConfig(o *consumerOption) (*sarama.Config, error) {
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

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	partition := claim.Partition()
	log.Info("start consume claim",
		zap.String("topic", claim.Topic()), zap.Int32("partition", partition),
		zap.Int64("initialOffset", claim.InitialOffset()), zap.Int64("highWaterMarkOffset", claim.HighWaterMarkOffset()))

	// TODO: mark the offset after the DDL is fully synced to the downstream mysql
	markMsg := func(message *sarama.ConsumerMessage) func() {
		return func() {
			session.MarkMessage(message, "")
		}
	}
	return c.KafkaConsume(partition, func(handle HandleFunc) error {
		for message := range claim.Messages() {
			err := handle(message.Key, message.Value, markMsg(message))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func SaramGetPartitionNum(address []string, topic string) (int32, error) {
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

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the c as ready
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// /**
// * Construct a new Sarama configuration.
// * The Kafka cluster version has to be defined before the consumer/producer is initialized.
// */
func SaramConsume(ctx context.Context, consumer *Consumer, wg *sync.WaitGroup) {
	consumerOption := consumer.option
	config, err := newConfig(consumerOption) // need change
	if err != nil {
		log.Panic("Error creating sarama config", zap.Error(err))
	}
	// wait topic create
	err = saramWaitTopicCreated(consumerOption.address, consumerOption.topic, config)
	if err != nil {
		log.Panic("wait topic created failed", zap.Error(err))
	}

	client, err := newConsumerGroup(consumerOption, config)
	if err != nil {
		log.Panic("Error creating consumer group client", zap.Error(err))
	}

	defer func() {
		if err = client.Close(); err != nil {
			log.Panic("Error closing client", zap.Error(err))
		}
	}()
	defer wg.Done()

	for {
		// `consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := client.Consume(ctx, strings.Split(consumerOption.topic, ","), consumer); err != nil {
			log.Panic("Error from consumer", zap.Error(err))
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			return
		}
		consumer.ready = make(chan bool)
	}
}
