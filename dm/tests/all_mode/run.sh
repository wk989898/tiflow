#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"
ILLEGAL_CHAR_NAME='t-Ë!s`t'

function test_session_config() {
	echo "[$(date)] <<<<<< start test_session_config >>>>>>"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i "s/name: test/name: $ILLEGAL_CHAR_NAME/g" $WORK_DIR/dm-task.yaml

	# error config
	# there should be a error message like "Incorrect argument type to variable 'tidb_retry_limit'"
	# but different TiDB version output different message. so we only roughly match here
	sed -i 's/tidb_retry_limit: "10"/tidb_retry_limit: "fjs"/g' $WORK_DIR/dm-task.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task.yaml --remove-meta" \
		"tidb_retry_limit" 1

	sed -i 's/tidb_retry_limit: "fjs"/tidb_retry_limit: "10"/g' $WORK_DIR/dm-task.yaml
	dmctl_start_task "$WORK_DIR/dm-task.yaml" "--remove-meta"
	run_sql_source1 "create table if not exists all_mode.t1 (c int); insert into all_mode.t1 (id, name) values (9, 'haha');"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_source1 "set session binlog_row_image = 'minimal'; update all_mode.t1 set name = 'hoho' where id = 9;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $ILLEGAL_CHAR_NAME" \
		"upstream didn't log enough columns in binlog" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task $ILLEGAL_CHAR_NAME" \
		"\"result\": true" 3

	cleanup_data all_mode
	cleanup_process
	echo "[$(date)] <<<<<< finish test_session_config >>>>>>"

}

function test_query_timeout() {
	echo "[$(date)] <<<<<< start test_query_timeout >>>>>>"
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/BlockSyncStatus=return(\"5s\")"

	cp $cur/conf/dm-master.toml $WORK_DIR/dm-master.toml
	sed -i 's/rpc-timeout = "30s"/rpc-timeout = "3s"/g' $WORK_DIR/dm-master.toml

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $WORK_DIR/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	sed -i "s/enable-relay: true/enable-relay: false/g" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	# don't know why CI has turned on Event Scheduler
	run_sql_both_source 'SET GLOBAL event_scheduler = OFF;'

	# there's only 1 row in result, which is for SHOW PROCESSLIST
	run_sql_source1 'SHOW PROCESSLIST;'
	check_rows_equal 1

	run_sql_source2 'SHOW PROCESSLIST;'
	check_rows_equal 1

	run_sql_tidb 'SHOW PROCESSLIST;'
	check_rows_equal 1

	# start DM task only
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i "s/name: test/name: $ILLEGAL_CHAR_NAME/g" $WORK_DIR/dm-task.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task.yaml --remove-meta"
	check_metric $WORKER1_PORT "dm_worker_task_state{source_id=\"mysql-replica-01\",task=\"$ILLEGAL_CHAR_NAME\",worker=\"worker1\"}" 10 1 3

	# `query-status` timeout
	start_time=$(date +%s)
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $ILLEGAL_CHAR_NAME" \
		"context deadline exceeded" 2
	duration=$(($(date +%s) - $start_time))
	if [[ $duration -gt 10 ]]; then
		echo "query-status takes too much time $duration"
		exit 1
	fi

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task $ILLEGAL_CHAR_NAME" \
		"\"result\": true" 3

	# there's only 1 row in result, which is for SHOW PROCESSLIST
	run_sql_source1 'SHOW PROCESSLIST;'
	check_rows_equal 1

	run_sql_source2 'SHOW PROCESSLIST;'
	check_rows_equal 1

	run_sql_tidb 'SHOW PROCESSLIST;'
	check_rows_equal 1

	cleanup_process
	cleanup_data all_mode

	export GO_FAILPOINTS=''
	echo "[$(date)] <<<<<< finish test_query_timeout >>>>>>"
}

function test_fail_job_between_event() {
	echo "[$(date)] <<<<<< start test_fail_job_between_event >>>>>>"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "s/enable-gtid: true/enable-gtid: false/g" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml

	# worker1 will be bound to source1 and fail when see the second row change in an event
	inject_points=(
		"github.com/pingcap/tiflow/dm/worker/TaskCheckInterval=return(\"500ms\")"
		"github.com/pingcap/tiflow/dm/syncer/countJobFromOneEvent=return()"
		"github.com/pingcap/tiflow/dm/syncer/flushFirstJob=return()"
		"github.com/pingcap/tiflow/dm/syncer/failSecondJob=return()"
	)
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	# worker2 will be bound to source2 and fail when see the second event in a GTID
	inject_points=(
		"github.com/pingcap/tiflow/dm/worker/TaskCheckInterval=return(\"500ms\")"
		"github.com/pingcap/tiflow/dm/syncer/countJobFromOneGTID=return()"
		"github.com/pingcap/tiflow/dm/syncer/flushFirstJob=return()"
		"github.com/pingcap/tiflow/dm/syncer/failSecondJob=return()"
	)
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	dmctl_start_task "$cur/conf/dm-task.yaml" "--remove-meta"

	run_sql_file $cur/data/db1.increment3.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment3.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_log_contain_with_retry "failSecondJob" $WORK_DIR/worker1/log/dm-worker.log
	check_log_contain_with_retry "failSecondJob" $WORK_DIR/worker2/log/dm-worker.log
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	cleanup_process
	cleanup_data all_mode

	export GO_FAILPOINTS=''
	echo "[$(date)] <<<<<< finish test_fail_job_between_event >>>>>>"
}

function test_expression_filter() {
	echo "[$(date)] <<<<<< start test_expression_filter >>>>>>"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "s/enable-gtid: true/enable-gtid: false/g" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	dmctl_start_task "$cur/conf/dm-task-expression-filter.yaml" "--remove-meta"

	run_sql_file $cur/data/db1.increment3.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment3.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_sql_tidb "SELECT count(*) from all_mode.t1;"
	check_contains "count(*): 6"
	run_sql_source1 "DELETE FROM all_mode.t1 WHERE id = 30;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3 "fail"
	run_sql_tidb "SELECT count(*) from all_mode.t1;"
	check_contains "count(*): 6"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3

	cleanup_process
	cleanup_data all_mode
	echo "[$(date)] <<<<<< finish test_expression_filter >>>>>>"
}

function test_regexpr_router() {
	echo "[$(date)] <<<<<< start test_regexpr_router $1 >>>>>>"
	cleanup_process
	cleanup_data all_mode
	cleanup_data test2animal
	cleanup_data test4s_2022
	cleanup_data_upstream test2animal
	cleanup_data_upstream test4s_2022
	run_sql_file $cur/data/db1.regexpr.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.regexpr.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql_tidb 'drop database if exists dtest2;'
	run_sql_tidb 'create database dtest2;'
	run_sql_tidb 'drop database if exists dtest4;'
	run_sql_tidb 'create database dtest4;'
	run_sql_tidb 'create table if not exists dtest2.dtable2(a int, b int);'
	run_sql_tidb 'create table if not exists dtest4.dtable4(a int, b int);'
	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2
	cp $cur/conf/$1 $WORK_DIR/$1
	sed -i "s/name: test/name: $ILLEGAL_CHAR_NAME/g" $WORK_DIR/$1

	# error config
	# there should be a error message like "Incorrect argument type to variable 'tidb_retry_limit'"
	# but different TiDB version output different message. so we only roughly match here
	sed -i 's/tidb_retry_limit: "10"/tidb_retry_limit: "fjs"/g' $WORK_DIR/$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/$1 --remove-meta" \
		"tidb_retry_limit" 1

	sed -i 's/tidb_retry_limit: "fjs"/tidb_retry_limit: "10"/g' $WORK_DIR/$1
	dmctl_start_task "$WORK_DIR/$1" "--remove-meta"

	check_sync_diff $WORK_DIR $cur/conf/regexpr_diff_config.toml

	cleanup_process
	cleanup_data test2animal
	cleanup_data test4s_2022
	cleanup_data_upstream test2animal
	cleanup_data_upstream test4s_2022
	echo "[$(date)] <<<<<< finish test_regexpr_router $1 >>>>>>"
}

function test_json_expression() {
	echo "[$(date)] <<<<<< start test_json_expression >>>>>>"
	run_sql_file $cur/data/db3.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "s/enable-gtid: true/enable-gtid: false/g" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	dmctl_start_task "$cur/conf/dm-task-expression-filter.yaml" "--remove-meta"

	run_sql_file $cur/data/db3.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	cleanup_process
	cleanup_data all_mode
	echo "[$(date)] <<<<<< finish test_json_expression >>>>>>"
}

function run() {
	run_sql_both_source "SET @@GLOBAL.SQL_MODE='ANSI_QUOTES,NO_AUTO_VALUE_ON_ZERO'"
	run_sql_source1 "SET @@global.time_zone = '+01:00';"
	run_sql_source2 "SET @@global.time_zone = '+02:00';"
	test_expression_filter
	test_json_expression
	test_fail_job_between_event
	test_session_config
	test_query_timeout
	test_regexpr_router regexpr-task.yaml
	test_regexpr_router regexpr-task-lightning.yaml

	inject_points=(
		"github.com/pingcap/tiflow/dm/worker/TaskCheckInterval=return(\"500ms\")"
		"github.com/pingcap/tiflow/dm/relay/NewUpstreamServer=return(true)"
	)
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"

	# manually create target table with two extra field
	run_sql_tidb "drop database if exists all_mode;"
	run_sql_tidb "create database all_mode;"
	run_sql_tidb "drop table if exists all_mode.no_diff;"
	run_sql_tidb "create table all_mode.no_diff(id int NOT NULL PRIMARY KEY, dt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP);"

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	# create table with unsupported charset
	run_sql_source1 "create table all_mode.no_diff2(id int primary key, name varchar(20)) charset=greek;"
	run_sql_source1 "insert into all_mode.no_diff2 values(1, 'αβγ');"

	# start DM worker and master
	# set log level of DM-master to info, because debug level will let etcd print KV, thus expose the password in task config
	run_dm_master_info_log $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	# make sure source1 is bound to worker1
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	# start DM task only
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i "s/name: test/name: $ILLEGAL_CHAR_NAME/g" $WORK_DIR/dm-task.yaml

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task.yaml --remove-meta" \
		"Unknown character set: 'greek'"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $ILLEGAL_CHAR_NAME" \
		"Unknown character set: 'greek'" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task $WORK_DIR/dm-task.yaml"

	run_sql_tidb "create table all_mode.no_diff2(id int primary key, name varchar(20)) charset=utf8mb4;"

	dmctl_start_task "$WORK_DIR/dm-task.yaml" "--remove-meta"
	# check task has started
	check_metric $WORKER1_PORT "dm_worker_task_state{source_id=\"mysql-replica-01\",task=\"$ILLEGAL_CHAR_NAME\",worker=\"worker1\"}" 10 1 3
	check_metric $WORKER2_PORT "dm_worker_task_state{source_id=\"mysql-replica-02\",task=\"$ILLEGAL_CHAR_NAME\",worker=\"worker2\"}" 10 1 3

	# use sync_diff_inspector to check full dump loader
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	# check data of no_diff2
	run_sql_tidb_with_retry "select count(1) from all_mode.no_diff2 where name = 'αβγ'" "count(1): 1"

	# check create view(should be skipped by func `skipSQLByPattern`) will not stop sync task
	run_sql_source1 "create view all_mode.t1_v as select * from all_mode.t1 where id=0;"
	sleep 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1" \
		"\"result\": true" 2 \
		"\"unit\": \"Sync\"" 1 \
		"\"stage\": \"Running\"" 2

	run_sql_source1 "SHOW SLAVE HOSTS;"
	check_contains 'Slave_UUID'

	run_sql_tidb "set time_zone = '+04:00';SELECT count(*) from all_mode.no_diff where dt = ts;"
	check_contains "count(*): 3"

	# check default session config
	check_log_contain_with_retry '\\"tidb_txn_mode\\":\\"optimistic\\"' $WORK_DIR/worker1/log/dm-worker.log
	check_log_contain_with_retry '\\"tidb_txn_mode\\":\\"optimistic\\"' $WORK_DIR/worker2/log/dm-worker.log

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-task $ILLEGAL_CHAR_NAME" \
		"\"result\": true" 3
	echo 'create table all_mode.no_diff(id int NOT NULL PRIMARY KEY);' >${WORK_DIR}/schema.sql
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-schema set -s mysql-replica-01 $ILLEGAL_CHAR_NAME -d all_mode -t no_diff ${WORK_DIR}/schema.sql" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "resume-task $ILLEGAL_CHAR_NAME" "\"result\": true" 3

	# restart dm-worker1
	pkill -hup -f dm-worker1.toml 2>/dev/null || true
	wait_pattern_exit dm-worker1.toml
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# make sure worker1 have bound a source, and the source should same with bound before
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $ILLEGAL_CHAR_NAME" \
		"worker1" 1

	# restart dm-worker2
	pkill -hup -f dm-worker2.toml 2>/dev/null || true
	wait_pattern_exit dm-worker2.toml
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	check_metric $WORKER1_PORT "dm_worker_task_state{source_id=\"mysql-replica-01\",task=\"$ILLEGAL_CHAR_NAME\",worker=\"worker1\"}" 10 1 3
	check_metric $WORKER2_PORT "dm_worker_task_state{source_id=\"mysql-replica-02\",task=\"$ILLEGAL_CHAR_NAME\",worker=\"worker2\"}" 10 1 3

	sleep 10
	echo "after restart dm-worker, task should resume automatically"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task.yaml" \
		"\"result\": false" 1 \
		"subtasks with name $ILLEGAL_CHAR_NAME for sources \[mysql-replica-01 mysql-replica-02\] already exist" 1
	sleep 2

	# wait for task running
	check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/$ILLEGAL_CHAR_NAME '"stage": "Running"' 10
	sleep 2 # still wait for subtask running on other dm-workers

	# kill tidb
	pkill -hup tidb-server 2>/dev/null || true
	wait_process_exit tidb-server

	# dm-worker execute sql failed, and will try auto resume task
	run_sql_file $cur/data/db2.increment0.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	sleep 45
	check_log_contains $WORK_DIR/worker2/log/dm-worker.log "dispatch auto resume task"

	# restart tidb, and task will recover success
	run_tidb_server 4000 $TIDB_PASSWORD
	sleep 2

	# test after pause and resume relay, relay could continue from syncer's checkpoint
	run_sql_source1 "flush logs"
	run_sql_file $cur/data/db1.increment0.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# check compatibility after incremental sync
	run_sql_tidb "set time_zone = '+04:00';SELECT count(*) from all_mode.no_diff where dt = ts;"
	check_contains "count(*): 4"

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-relay -s mysql-replica-01" \
		"\"result\": true" 2
	# we used failpoint to imitate an upstream switching, which purged whole relay dir
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-relay -s mysql-replica-01" \
		"\"result\": true" 2

	sleep 2
	# relay should continue pulling from syncer's checkpoint, so only pull the latest binlog
	server_uuid=$(tail -n 1 $WORK_DIR/worker1/relay_log/server-uuid.index)
	echo "relay logs $(ls $WORK_DIR/worker1/relay_log/$server_uuid)"
	relay_log_num=$(ls $WORK_DIR/worker1/relay_log/$server_uuid | grep -v 'relay.meta' | wc -l)
	[ $relay_log_num -eq 1 ]

	# use sync_diff_inspector to check data now!
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# test block-allow-list by the way
	run_sql "show databases;" $TIDB_PORT $TIDB_PASSWORD
	check_not_contains "ignore_db"
	check_contains "all_mode"

	echo "check dump files have been cleaned"
	# source1 contains unsupported charset, so dump files is uncleaned. files are
	# all_mode.no_diff2-schema.sql  all_mode.t1-schema.sql
	# all_mode.no_diff-schema.sql   metadata
	# all_mode-schema-create.sql
	[ $(ls $WORK_DIR/worker1/dumped_data.$ILLEGAL_CHAR_NAME | wc -l) -eq 5 ]
	ls $WORK_DIR/worker2/dumped_data.$ILLEGAL_CHAR_NAME && exit 1 || echo "worker2 auto removed dump files"

	echo "check no password in log"
	check_log_not_contains $WORK_DIR/master/log/dm-master.log "123456"
	check_log_not_contains $WORK_DIR/worker1/log/dm-worker.log "123456"
	check_log_not_contains $WORK_DIR/worker2/log/dm-worker.log "123456"

	# test drop table if exists
	run_sql_source1 "drop table if exists \`all_mode\`.\`tb1\`;"
	run_sql_source1 "drop table if exists \`all_mode\`.\`tb1\`;"
	run_sql_source2 "drop table if exists \`all_mode\`.\`tb2\`;"
	run_sql_source2 "drop table if exists \`all_mode\`.\`tb2\`;"

	# test Db not exists should be reported
	run_sql_tidb "drop database all_mode"
	run_sql_source1 "create table all_mode.db_error (c int primary key);"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $ILLEGAL_CHAR_NAME" \
		"Error 1049 (42000): Unknown database" 1

	# stop task, task state should be cleaned
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task $ILLEGAL_CHAR_NAME" \
		"\"result\": true" 3
	check_metric_not_contains $WORKER1_PORT "dm_worker_task_state{source_id=\"mysql-replica-01\",task=\"$ILLEGAL_CHAR_NAME\",worker=\"worker1\"}" 3
	check_metric_not_contains $WORKER2_PORT "dm_worker_task_state{source_id=\"mysql-replica-02\",task=\"$ILLEGAL_CHAR_NAME\",worker=\"worker2\"}" 3

	# all unit without error.
	check_metric_not_contains $WORKER1_PORT "dm_mydumper_exit_with_error_count" 3
	check_metric_not_contains $WORKER1_PORT "dm_loader_exit_with_error_count" 3
	check_metric_not_contains $WORKER1_PORT "dm_syncer_exit_with_error_count" 3

	# check syncer metrics
	check_two_metric_equal $WORKER1_PORT 'dm_syncer_binlog_file{node="master"' 'dm_syncer_binlog_file{node="syncer"' 3
	check_two_metric_equal $WORKER2_PORT 'dm_syncer_binlog_file{node="master"' 'dm_syncer_binlog_file{node="syncer"' 3
	export GO_FAILPOINTS=''

	run_sql_both_source "SET @@GLOBAL.SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
	run_sql_both_source "SET @@global.time_zone = 'SYSTEM';"

	test_source_and_target_with_empty_gtid
}

function prepare_test_empty_gtid() {
	run_sql 'DROP DATABASE if exists all_mode;' $TIDB_PORT $TIDB_PASSWORD
	run_sql 'DROP DATABASE if exists all_mode;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'DROP DATABASE if exists xxx;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'CREATE DATABASE all_mode;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "CREATE TABLE all_mode.t1(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_sql 'reset master;' $MYSQL_PORT1 $MYSQL_PASSWORD1
}

function test_source_and_target_with_empty_gtid() {
	echo "[$(date)] <<<<<< start test_source_and_target_with_empty_gtid >>>>>>"
	cleanup_process
	cleanup_data all_mode
	prepare_test_empty_gtid

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/dm-master.toml $WORK_DIR/
	cp $cur/conf/dm-worker1.toml $WORK_DIR/
	cp $cur/conf/dm-task-no-gtid.yaml $WORK_DIR/

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $WORK_DIR/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $WORK_DIR/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# operate mysql config to worker
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source create $WORK_DIR/source1.yaml" \
		"\"result\": true" 2 \
		"\"source\": \"$SOURCE_ID1\"" 1

	echo "check master alive"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member" \
		"\"alive\": true" 1

	# check current master gtid is empty
	# len indicates the number of non-empty fields
	len=$(echo "show master status;" | MYSQL_PWD=$MYSQL_PASSWORD1 mysql -uroot -h127.0.0.1 -P$MYSQL_PORT1 | awk 'FNR == 2 {print NF}')
	if [ "$len" = 2 ]; then
		echo "gtid is empty"
	else
		exit 1
	fi

	echo "start task and check stage"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task-no-gtid.yaml --remove-meta=true" \
		"\"result\": true" 2

	run_sql 'INSERT INTO all_mode.t1 VALUES (1,1001);' $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		"\"unit\": \"Sync\"" 1 \
		"\"stage\": \"Running\"" 2

	echo "check data"
	check_sync_diff $WORK_DIR $cur/conf/diff_config-1.toml

	# check checkpoint matches master when the last event is a ddl
	# 1. ddl that dm will sync
	run_sql_source1 "create table all_mode.t2(c int primary key)"
	run_sql_tidb_with_retry "show create table all_mode.t2" "CREATE TABLE"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		'"synced": true' 1
	# 2. ddl cannot be parsed and should be skipped
	run_sql_source1 "create FUNCTION all_mode.hello (s CHAR(20)) RETURNS CHAR(50) DETERMINISTIC RETURN 'a';"
	check_log_contain_with_retry "RETURNS char(50)" $WORK_DIR/worker1/log/dm-worker.log
	sleep 30 # we rely on heartbeat event to flush checkpoint here, below too
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		'"synced": true' 1
	# 3. ddl can be parsed and dm don't handle
	run_sql_source1 "analyze table all_mode.t1"
	check_log_contain_with_retry "analyze table" $WORK_DIR/worker1/log/dm-worker.log
	sleep 30
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		'"synced": true' 1
	# 4. ddl that is filtered
	run_sql_source1 "create database xxx"
	check_log_contain_with_retry "CREATE DATABASE IF NOT EXISTS" $WORK_DIR/worker1/log/dm-worker.log
	sleep 30
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		'"synced": true' 1

	echo "<<<<<< test_source_and_target_with_empty_gtid success! >>>>>>"
}

cleanup_data_upstream all_mode
cleanup_data all_mode
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
