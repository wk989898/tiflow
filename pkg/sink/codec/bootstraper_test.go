// Copyright 2023 PingCAP, Inc.
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

package codec

import (
	"context"
	"fmt"
	"testing"
	"time"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func getMockTableStatus(tableName string,
	tableID int64,
	totalPartition int32,
) (model.TopicPartitionKey, *model.RowChangedEvent, *tableStatistic) {
	schema := "test"
	tableInfo := &model.TableInfo{
		TableName: model.TableName{
			Schema:  schema,
			Table:   tableName,
			TableID: tableID,
		},
		TableInfo: &timodel.TableInfo{
			ID:       tableID,
			UpdateTS: 1,
		},
	}
	key := model.TopicPartitionKey{
		Topic:          fmt.Sprintf("%s.%s", schema, tableName),
		Partition:      1,
		TotalPartition: totalPartition,
	}
	row := &model.RowChangedEvent{
		PhysicalTableID: tableID,
		TableInfo:       tableInfo,
	}
	tb := newTableStatistic(key, row)
	return key, row, tb
}

func TestShouldSendBootstrapMsg(t *testing.T) {
	t.Parallel()
	defaultSendBootstrapInterval := time.Duration(config.DefaultSendBootstrapIntervalInSec) * time.Second
	defaultSendBootstrapInMsgCount := config.DefaultSendBootstrapInMsgCount

	_, _, tb1 := getMockTableStatus("t1", int64(1), int32(3))

	// case 1: A new added table should send bootstrap message immediately
	require.True(t, tb1.
		shouldSendBootstrapMsg(defaultSendBootstrapInterval, defaultSendBootstrapInMsgCount))

	// case 2: A table which has sent bootstrap message should not send bootstrap message
	tb1.lastSendTime.Store(time.Now())
	require.False(t, tb1.shouldSendBootstrapMsg(defaultSendBootstrapInterval, defaultSendBootstrapInMsgCount))

	// case 3: When the table receive message more than sendBootstrapInMsgCount,
	// it should send bootstrap message
	tb1.counter.Add(defaultSendBootstrapInMsgCount)
	require.True(t, tb1.shouldSendBootstrapMsg(defaultSendBootstrapInterval, defaultSendBootstrapInMsgCount))

	// case 4: When the table does not send bootstrap message for a sendBootstrapInterval time,
	// it should send bootstrap message
	tb1.lastSendTime.Store(time.Now().Add(-defaultSendBootstrapInterval))
	require.True(t, tb1.shouldSendBootstrapMsg(defaultSendBootstrapInterval, defaultSendBootstrapInMsgCount))
}

func TestIsActive(t *testing.T) {
	t.Parallel()
	_, row, tb1 := getMockTableStatus("t1", int64(1), int32(3))
	// case 1: A new added table should be active
	require.False(t, tb1.isInactive(defaultMaxInactiveDuration))

	// case 2: A table which does not receive message for a long time should be inactive
	tb1.lastMsgReceivedTime.Store(time.Now().Add(-defaultMaxInactiveDuration))
	require.True(t, tb1.isInactive(defaultMaxInactiveDuration))

	// case 3: A table which receive message recently should be active
	// Note: A table's update method will be call any time it receive message
	// So use update method to simulate the table receive message
	tb1.update(row, 1)
	require.False(t, tb1.isInactive(defaultMaxInactiveDuration))
}

func TestBootstrapWorker(t *testing.T) {
	t.Parallel()
	// new builder
	cfID := model.DefaultChangeFeedID("test")
	builder := &MockRowEventEncoderBuilder{}
	outCh := make(chan *future, defaultInputChanSize)
	worker := newBootstrapWorker(
		cfID,
		outCh,
		builder.Build(),
		config.DefaultSendBootstrapIntervalInSec,
		config.DefaultSendBootstrapInMsgCount,
		false,
		defaultMaxInactiveDuration)

	// Start the worker in a separate goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = worker.run(ctx)
	}()

	// case 1: A new added table should send bootstrap message immediately
	// Configure `sendBootstrapToAllPartition` to false
	// The bootstrap message number should be equal to 1
	// Event if we send the same table twice, it should only send bootstrap message once
	key1, row1, _ := getMockTableStatus("t1", int64(1), int32(3))
	err := worker.addEvent(ctx, key1, row1)
	require.NoError(t, err)
	err = worker.addEvent(ctx, key1, row1)
	require.NoError(t, err)
	var msgCount int32
	c1ctx, c1Cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer c1Cancel()
l1:
	for {
		select {
		case future := <-outCh:
			require.NotNil(t, future)
			require.Equal(t, key1.Topic, future.Key.Topic)
			msgCount++
			if msgCount == 1 {
				break l1
			}
		case <-c1ctx.Done():
			break l1
		}
	}
	// The bootstrap event is only sent to the first partition
	require.Equal(t, int32(1), msgCount)

	// case 2: Configure `sendBootstrapToAllPartition` to true
	// The messages number should be equal to the total partition number
	worker.sendBootstrapToAllPartition = true
	key2, row2, _ := getMockTableStatus("t2", int64(2), int32(2))
	err = worker.addEvent(ctx, key2, row2)
	require.NoError(t, err)
	err = worker.addEvent(ctx, key2, row2)
	require.NoError(t, err)
	msgCount = 0
	c2ctx, c2Cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer c2Cancel()
l2:
	for {
		select {
		case future := <-outCh:
			require.NotNil(t, future)
			require.Equal(t, key2.Topic, future.Key.Topic)
			msgCount++
			if msgCount == key2.TotalPartition {
				break l2
			}
		case <-c2ctx.Done():
			break l2
		}
	}
	// The bootstrap events are sent to all partition
	require.Equal(t, key2.TotalPartition, msgCount)
}

func TestUpdateTableStatistic(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(
		id int primary key,
		name varchar(64) not null,
		age int,
		email varchar(255) not null,
		unique index idx_name(name),
		index idx_age_email(age,email)
	);`
	tableInfo1 := helper.DDL2Event(sql).TableInfo
	row1 := &model.RowChangedEvent{
		PhysicalTableID: tableInfo1.ID,
		TableInfo:       tableInfo1,
	}
	tableStatistic := newTableStatistic(model.TopicPartitionKey{}, row1)

	// case 1: The tableStatistic should not be updated if the tableInfo is the same
	tableStatistic.update(row1, 1)
	require.Equal(t, tableInfo1, tableStatistic.tableInfo.Load().(*model.TableInfo))

	// case 2: The tableStatistic should be updated if the tableInfo is different
	sql = `alter table test.t1 add column address varchar(255) not null;`
	tableInfo2 := helper.DDL2Event(sql).TableInfo
	row2 := &model.RowChangedEvent{
		PhysicalTableID: tableInfo2.ID,
		TableInfo:       tableInfo2,
	}
	tableStatistic.update(row2, 1)
	require.Equal(t, tableInfo2, tableStatistic.tableInfo.Load().(*model.TableInfo))

	// case 3: The tableStatistic should be updated when rename table
	sql = `alter table test.t1 rename to test.t2;`
	tableInfo3 := helper.DDL2Event(sql).TableInfo
	row3 := &model.RowChangedEvent{
		PhysicalTableID: tableInfo3.ID,
		TableInfo:       tableInfo3,
	}
	tableStatistic.update(row3, 1)
	require.Equal(t, tableInfo3, tableStatistic.tableInfo.Load().(*model.TableInfo))
}
