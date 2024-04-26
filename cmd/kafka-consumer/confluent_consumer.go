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
	"fmt"
	"os"
	"strings"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func confluentGetPartitionNum(address []string, topic string) (int32, error) {
	// client := &confluent.Client{
	// 	Addr:    kafka.TCP(address...),
	// 	Timeout: 10 * time.Second,
	// }
	// resp, err := client.Metadata(context.Background(), &confluent.MetadataRequest{
	// 	Addr:   client.Addr,
	// 	Topics: []string{topic},
	// })
	// if err != nil {
	// 	return 0, cerror.Trace(err)
	// }
	// topics := resp.Topics
	// var (
	// 	topicDetail confluent.Topic
	// 	exist       bool
	// )
	// for i := 0; i < len(topics); i++ {
	// 	if topics[i].Name == topic {
	// 		topicDetail = topics[i]
	// 		exist = true
	// 		break
	// 	}
	// }
	// if !exist {
	// 	return 0, cerror.Errorf("can not find topic %s", topic)
	// }
	// numPartitions := int32(len(topicDetail.Partitions))
	// log.Info("get partition number of topic",
	// 	zap.String("topic", topic),
	// 	zap.Int32("partitionNum", numPartitions))
	// return numPartitions, nil
}

func confluentWaitTopicCreated(address []string, topic string) error {
	// client := &kafka.Client{
	// 	Addr: kafka.TCP(address...),
	// 	// todo: make this configurable
	// 	Timeout: 10 * time.Second,
	// 	// Transport: cfg. transport,
	// }
	// for i := 0; i <= 30; i++ {
	// 	resp, err := client.Metadata(context.Background(), &kafka.MetadataRequest{})
	// 	if err != nil {
	// 		return cerror.Trace(err)
	// 	}
	// 	topics := resp.Topics
	// 	for i := 0; i < len(topics); i++ {
	// 		if topics[i].Name == topic {
	// 			return nil
	// 		}
	// 	}
	// 	log.Info("wait the topic created", zap.String("topic", topic))
	// 	time.Sleep(1 * time.Second)
	// }
	// return cerror.Errorf("wait the topic(%s) created timeout", topic)

}

func ConfluentConsume() {
	// bootstrapServers := os.Args[1]
	// group := os.Args[2]
	// topics := os.Args[3:]
	// sigchan := make(chan os.Signal, 1)
	// signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
	}

	fmt.Printf("Created Consumer %v\n", consumer)

	err = consumer.SubscribeTopics(topics, nil)

	run := true

	fmt.Printf("Closing consumer\n")
}

type ConfluentConsumer struct {
	option *ConsumerOption
	writer *Writer
}

var _ KakfaConsumer = (*ConfluentConsumer)(nil)

func NewConfluentConsumer(ctx context.Context, o *ConsumerOption) KakfaConsumer {
	c := new(ConfluentConsumer)
	w, err := NewWriter(ctx, o)
	if err != nil {
		log.Panic("Error creating writer", zap.Error(err))
	}
	c.writer = w
	if o.partitionNum != 0 {
		partitionNum, err := confluentGetPartitionNum(o.address, o.topic)
		if err != nil {
			log.Panic("can not get partition number", zap.String("topic", o.topic), zap.Error(err))
		}
		o.partitionNum = partitionNum
	}
	c.option = o

	err = confluentWaitTopicCreated(o.address, o.topic)
	// wait topic create
	if err != nil {
		log.Panic("wait topic created failed", zap.Error(err))
	}
	return c
}

func (c *ConfluentConsumer) Consume(ctx context.Context) error {
	consumerOption := c.option
	// wait topic create
	err := confluentWaitTopicCreated(consumerOption.address, consumerOption.topic)
	if err != nil {
		log.Panic("wait topic created failed", zap.Error(err))
	}
	topics := strings.Split(consumerOption.topic, ",")
	if len(topics) == 0 {
		log.Panic("no topics provided")
	}
	client, err := confluent.NewConsumer(&confluent.ConfigMap{
		"bootstrap.servers": consumerOption.address,
		// Avoid connecting to IPv6 brokers:
		// This is needed for the ErrAllBrokersDown show-case below
		// when using localhost brokers on OSX, since the OSX resolver
		// will return the IPv6 addresses first.
		// You typically don't need to specify this configuration property.
		"broker.address.family": "v4",
		"group.id":              consumerOption.groupID,
		"session.timeout.ms":    6000,
		// Start reading from the first message of each assigned
		// partition if there are no previously committed offsets
		// for this group.
		"auto.offset.reset": "earliest",
		// Whether or not we store offsets automatically.
		"enable.auto.offset.store": false,
	})
	if err != nil {
		log.Panic("Error creating consumer group client", zap.Error(err))
	}
	defer func() {
		if err = client.Close(); err != nil {
			log.Panic("Error closing client", zap.Error(err))
		}
	}()
}

// async write to downsteam
func (c *ConfluentConsumer) AsyncWrite(ctx context.Context) {
	c.writer.AsyncWrite(ctx)
}
