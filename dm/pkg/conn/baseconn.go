// Copyright 2019 PingCAP, Inc.
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

package conn

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/errno"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// BaseConn is the basic connection we use in dm
// BaseDB -> BaseConn correspond to sql.DB -> sql.Conn
// In our scenario, there are two main reasons why we need BaseConn
//  1. we often need one fixed DB connection to execute sql
//  2. we need own retry policy during execute failed
//
// So we split a fixed sql.Conn out of sql.DB, and wraps it to BaseConn
// And Similar with sql.Conn, all BaseConn generated from one BaseDB shares this BaseDB to reset
//
// Basic usage:
// For Syncer and Loader Unit, they both have different amount of connections due to config
// Currently we have some types of connections exist
//
//	Syncer:
//		Worker Connection:
//			DML connection:
//				execute some DML on Downstream DB, one unit has `syncer.WorkerCount` worker connections
//			DDL Connection:
//				execute some DDL on Downstream DB, one unit has one connection
//		CheckPoint Connection:
//			interact with CheckPoint DB, one unit has one connection
//		OnlineDDL connection:
//			interact with Online DDL DB, one unit has one connection
//		ShardGroupKeeper connection:
//			interact with ShardGroupKeeper DB, one unit has one connection
//
//	Loader:
//		Worker Connection:
//			execute some DML to Downstream DB, one unit has `loader.PoolSize` worker connections
//		CheckPoint Connection:
//			interact with CheckPoint DB, one unit has one connection
//		Restore Connection:
//			only use to create schema and table in restoreData,
//			it ignore already exists error and it should be removed after use, one unit has one connection
//
// each connection should have ability to retry on some common errors (e.g. tmysql.ErrTiKVServerTimeout) or maybe some specify errors in the future
// and each connection also should have ability to reset itself during some specify connection error (e.g. driver.ErrBadConn).
type BaseConn struct {
	DBConn        *sql.Conn
	Scope         terror.ErrScope
	RetryStrategy retry.Strategy
}

// NewBaseConn builds BaseConn to connect real DB.
func NewBaseConn(conn *sql.Conn, scope terror.ErrScope, strategy retry.Strategy) *BaseConn {
	if strategy == nil {
		strategy = &retry.FiniteRetryStrategy{}
	}
	return &BaseConn{
		DBConn:        conn,
		Scope:         scope,
		RetryStrategy: strategy,
	}
}

// NewBaseConnForTest builds BaseConn to connect real DB for test.
func NewBaseConnForTest(conn *sql.Conn, strategy retry.Strategy) *BaseConn {
	if strategy == nil {
		strategy = &retry.FiniteRetryStrategy{}
	}
	return &BaseConn{
		DBConn:        conn,
		Scope:         terror.ScopeNotSet,
		RetryStrategy: strategy,
	}
}

// SetRetryStrategy set retry strategy for baseConn.
func (conn *BaseConn) SetRetryStrategy(strategy retry.Strategy) error {
	if conn == nil {
		return terror.ErrDBUnExpect.Generate("database connection not valid")
	}
	conn.RetryStrategy = strategy
	return nil
}

// QuerySQL runs a query statement.
func (conn *BaseConn) QuerySQL(tctx *tcontext.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if conn == nil || conn.DBConn == nil {
		return nil, terror.ErrDBUnExpect.Generate("database connection not valid")
	}
	tctx.L().Debug("query statement",
		zap.String("query", utils.TruncateString(query, -1)),
		log.ZapRedactString("argument", utils.TruncateInterface(args, -1)))

	rows, err := conn.DBConn.QueryContext(tctx.Context(), query, args...)
	if err != nil {
		tctx.L().ErrorFilterContextCanceled("query statement failed",
			zap.String("query", utils.TruncateString(query, -1)),
			log.ZapRedactString("argument", utils.TruncateInterface(args, -1)),
			log.ShortError(err))
		return nil, terror.ErrDBQueryFailed.Delegate(err, utils.TruncateString(query, -1))
	}
	return rows, nil
}

// ExecuteSQLWithIgnoreError executes sql on real DB, and will ignore some error and continue execute the next query.
// return
// 1. failed: (the index of sqls executed error, error)
// 2. succeed: (rows affected, nil).
func (conn *BaseConn) ExecuteSQLWithIgnoreError(tctx *tcontext.Context, hVec *prometheus.HistogramVec, task string, ignoreErr func(error) bool, queries []string, args ...[]interface{}) (int, error) {
	var affect int64
	// inject an error to trigger retry, this should be placed before the real execution of the SQL statement.
	failpoint.Inject("retryableError", func(val failpoint.Value) {
		if mark, ok := val.(string); ok {
			enabled := false
			for _, query := range queries {
				if strings.Contains(query, mark) {
					enabled = true // only enable if the `mark` matched.
				}
			}
			if enabled {
				tctx.L().Info("", zap.String("failpoint", "retryableError"), zap.String("mark", mark))
				failpoint.Return(0, &mysql.MySQLError{
					Number:  gmysql.ER_LOCK_DEADLOCK,
					Message: fmt.Sprintf("failpoint inject retryable error for %s", mark),
				})
			}
		}
	})

	if len(queries) == 0 {
		return 0, nil
	}
	if conn == nil || conn.DBConn == nil {
		return 0, terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	startTime := time.Now()
	txn, err := conn.DBConn.BeginTx(tctx.Context(), nil)
	if err != nil {
		return 0, terror.ErrDBExecuteFailedBegin.Delegate(err)
	}
	if hVec != nil {
		hVec.WithLabelValues("begin", task).Observe(time.Since(startTime).Seconds())
	}

	l := len(queries)

	for i, query := range queries {
		var arg []interface{}
		if len(args) > i {
			arg = args[i]
		}

		// avoid use TruncateInterface for all log level which will slow the speed of DML
		if tctx.L().Core().Enabled(zap.DebugLevel) {
			tctx.L().Debug("execute statement",
				zap.String("query", utils.TruncateString(query, -1)),
				log.ZapRedactString("argument", utils.TruncateInterface(arg, -1)))
		}

		startTime = time.Now()
		result, err2 := txn.ExecContext(tctx.Context(), query, arg...)
		if err2 == nil {
			rows, _ := result.RowsAffected()
			affect += rows
			if hVec != nil {
				hVec.WithLabelValues("stmt", task).Observe(time.Since(startTime).Seconds())
			}
		} else {
			if ignoreErr != nil && ignoreErr(err2) {
				tctx.L().Warn("execute statement failed and will ignore this error",
					zap.String("query", utils.TruncateString(query, -1)),
					log.ZapRedactString("argument", utils.TruncateInterface(arg, -1)),
					log.ShortError(err2))
				continue
			}

			tctx.L().ErrorFilterContextCanceled("execute statement failed",
				zap.String("query", utils.TruncateString(query, -1)),
				log.ZapRedactString("argument", utils.TruncateInterface(arg, -1)), log.ShortError(err2))

			startTime = time.Now()
			rerr := txn.Rollback()
			if rerr != nil {
				tctx.L().Error("rollback failed",
					zap.String("query", utils.TruncateString(query, -1)),
					log.ZapRedactString("argument", utils.TruncateInterface(arg, -1)),
					log.ShortError(rerr))
			} else if hVec != nil {
				hVec.WithLabelValues("rollback", task).Observe(time.Since(startTime).Seconds())
			}
			// we should return the exec err, instead of the rollback rerr.
			return i, terror.ErrDBExecuteFailed.Delegate(err2, utils.TruncateString(query, -1))
		}
	}
	startTime = time.Now()
	err = txn.Commit()
	if err != nil {
		return l - 1, terror.ErrDBExecuteFailed.Delegate(err, "commit") // mark failed on the last one
	}
	if hVec != nil {
		hVec.WithLabelValues("commit", task).Observe(time.Since(startTime).Seconds())
	}
	return int(affect), nil
}

// ExecuteSQL executes sql on real DB,
// return
// 1. failed: (the index of sqls executed error, error)
// 2. succeed: (rows affected, nil).
func (conn *BaseConn) ExecuteSQL(tctx *tcontext.Context, hVec *prometheus.HistogramVec, task string, queries []string, args ...[]interface{}) (int, error) {
	return conn.ExecuteSQLWithIgnoreError(tctx, hVec, task, nil, queries, args...)
}

// ExecuteSQLsAutoSplit executes sqls and when meet "transaction too large" error,
// it will try to split the sqls into two parts and execute them again.
// The `queries` and `args` should be the same length.
func (conn *BaseConn) ExecuteSQLsAutoSplit(
	tctx *tcontext.Context,
	hVec *prometheus.HistogramVec,
	task string,
	queries []string,
	args ...[]interface{},
) error {
	_, err := conn.ExecuteSQL(tctx, hVec, task, queries, args...)
	mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
	if !ok {
		return err
	}

	if mysqlErr.Number != errno.ErrTxnTooLarge || len(queries) == 1 {
		return err
	}

	mid := len(queries) / 2
	err = conn.ExecuteSQLsAutoSplit(tctx, hVec, task, queries[:mid], args[:mid]...)
	if err != nil {
		return err
	}
	return conn.ExecuteSQLsAutoSplit(tctx, hVec, task, queries[mid:], args[mid:]...)
}

// ApplyRetryStrategy apply specify strategy for BaseConn.
func (conn *BaseConn) ApplyRetryStrategy(tctx *tcontext.Context, params retry.Params,
	operateFn func(*tcontext.Context) (interface{}, error),
) (interface{}, int, error) {
	return conn.RetryStrategy.Apply(tctx, params, operateFn)
}

// close returns the connection to the connection pool, has the same meaning of sql.Conn.Close.
func (conn *BaseConn) close() error {
	if conn == nil || conn.DBConn == nil {
		return nil
	}
	return conn.DBConn.Close()
}

// forceClose will close the underlying connection completely,
// should not be used by functions other than BaseDB.ForceCloseConn.
func (conn *BaseConn) forceClose() error {
	if conn == nil || conn.DBConn == nil {
		return nil
	}

	err := conn.DBConn.Raw(func(dc interface{}) error {
		// return an `ErrBadConn` to ensure close the connection, but do not put it back to the pool.
		// if we choose to use `Close`, it will always put the connection back to the pool.
		return driver.ErrBadConn
	})
	if err != driver.ErrBadConn {
		return terror.ErrDBUnExpect.Delegate(err, "close")
	}
	return nil
}
