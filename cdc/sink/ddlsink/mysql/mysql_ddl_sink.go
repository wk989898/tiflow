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

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/go-sql-driver/mysql"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/dumpling/export"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/sink"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"go.uber.org/zap"
)

const (
	defaultDDLMaxRetry uint64 = 20

	// networkDriftDuration is used to construct a context timeout for database operations.
	networkDriftDuration = 5 * time.Second

	defaultSupportVectorVersion = "8.4.0"
)

// GetDBConnImpl is the implementation of pmysql.IDBConnectionFactory.
// Exported for testing.
var GetDBConnImpl pmysql.IDBConnectionFactory = &pmysql.DBConnectionFactory{}

// Assert Sink implementation
var _ ddlsink.Sink = (*DDLSink)(nil)

// DDLSink is a sink that writes DDL events to MySQL.
type DDLSink struct {
	// id indicates which processor (changefeed) this sink belongs to.
	id model.ChangeFeedID
	// db is the database connection.
	db  *sql.DB
	cfg *pmysql.Config
	// statistics is the statistics of this sink.
	// We use it to record the DDL count.
	statistics *metrics.Statistics

	// lastExecutedNormalDDLCache is a fast path to check whether aync DDL of a table
	// is running in downstream.
	// map: model.TableName -> timodel.ActionType
	lastExecutedNormalDDLCache *lru.Cache

	needFormat bool
}

// NewDDLSink creates a new DDLSink.
func NewDDLSink(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
) (*DDLSink, error) {
	cfg := pmysql.NewConfig()
	err := cfg.Apply(config.GetGlobalServerConfig().TZ, changefeedID, sinkURI, replicaConfig)
	if err != nil {
		return nil, err
	}

	dsnStr, err := pmysql.GenerateDSN(ctx, sinkURI, cfg, GetDBConnImpl.CreateTemporaryConnection)
	if err != nil {
		return nil, err
	}

	db, err := GetDBConnImpl.CreateStandardConnection(ctx, dsnStr)
	if err != nil {
		return nil, err
	}

	cfg.IsTiDB = pmysql.CheckIsTiDB(ctx, db)

	cfg.IsWriteSourceExisted, err = pmysql.CheckIfBDRModeIsSupported(ctx, db)
	if err != nil {
		return nil, err
	}

	lruCache, err := lru.New(1024)
	if err != nil {
		return nil, err
	}

	m := &DDLSink{
		id:                         changefeedID,
		db:                         db,
		cfg:                        cfg,
		statistics:                 metrics.NewStatistics(changefeedID, sink.TxnSink),
		lastExecutedNormalDDLCache: lruCache,
		needFormat:                 needFormatDDL(db, cfg),
	}

	log.Info("MySQL DDL sink is created",
		zap.String("namespace", m.id.Namespace),
		zap.String("changefeed", m.id.ID))
	return m, nil
}

// WriteDDLEvent writes a DDL event to the mysql database.
func (m *DDLSink) WriteDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	m.waitAsynExecDone(ctx, ddl)

	if m.shouldAsyncExecDDL(ddl) {
		m.lastExecutedNormalDDLCache.Remove(ddl.TableInfo.TableName)
		return m.asyncExecDDL(ctx, ddl)
	}

	ddlCreateTime := getDDLCreateTime(ctx, m.db)
	if err := m.execDDLWithMaxRetries(ctx, ddl); err != nil {
		if m.cfg.IsTiDB && ddlCreateTime != -1 && errors.Cause(err) == mysql.ErrInvalidConn {
			log.Warn("Wait the asynchronous ddl to synchronize", zap.String("ddl", ddl.Query), zap.Int64("ddlCreateTime", ddlCreateTime), zap.Error(err))
			ticker := time.NewTimer(30 * time.Second)
			defer ticker.Stop()
			for {
				status, err1 := getDDLStatusFromTiDB(ctx, m.db, ddl.Query, ddlCreateTime)
				if err1 != nil {
					log.Warn("Error when getting DDL status from TiDB", zap.Error(err1))
				}
				switch status {
				case timodel.JobStateDone.String(), timodel.JobStateSynced.String():
					log.Info("DDL replicate success", zap.String("ddl", ddl.Query), zap.Int64("ddlCreateTime", ddlCreateTime), zap.Error(err))
					return nil
				case timodel.JobStateCancelled.String(), timodel.JobStateRollingback.String(), timodel.JobStateRollbackDone.String(), timodel.JobStateCancelling.String():
					return errors.ErrExecDDLFailed.GenWithStackByArgs(ddl.Query)
				case timodel.JobStateRunning.String(), timodel.JobStateQueueing.String(), timodel.JobStateNone.String():
				default:
					log.Warn("Unexpected DDL status", zap.String("DDL status", status))
					return err
				}
				select {
				case <-ctx.Done():
					return err
				case <-ticker.C:
				}
			}
		}
		return errors.Trace(err)
	}
	m.lastExecutedNormalDDLCache.Add(ddl.TableInfo.TableName, ddl.Type)
	return nil
}

func (m *DDLSink) execDDLWithMaxRetries(ctx context.Context, ddl *model.DDLEvent) error {
	return retry.Do(ctx, func() error {
		err := m.statistics.RecordDDLExecution(func() error { return m.execDDL(ctx, ddl) })
		if err != nil {
			if errorutil.IsIgnorableMySQLDDLError(err) {
				// NOTE: don't change the log, some tests depend on it.
				log.Info("Execute DDL failed, but error can be ignored",
					zap.Uint64("startTs", ddl.StartTs), zap.String("ddl", ddl.Query),
					zap.String("namespace", m.id.Namespace),
					zap.String("changefeed", m.id.ID),
					zap.Error(err))
				// If the error is ignorable, we will ignore the error directly.
				return nil
			}
			log.Warn("Execute DDL with error, retry later",
				zap.Uint64("startTs", ddl.StartTs), zap.String("ddl", ddl.Query),
				zap.String("namespace", m.id.Namespace),
				zap.String("changefeed", m.id.ID),
				zap.Error(err))
			return err
		}
		return nil
	}, retry.WithBackoffBaseDelay(pmysql.BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(pmysql.BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(defaultDDLMaxRetry),
		retry.WithIsRetryableErr(errorutil.IsRetryableDDLError))
}

// isReorgOrPartitionDDL returns true if given ddl type is reorg ddl or
// partition ddl.
func isReorgOrPartitionDDL(t timodel.ActionType) bool {
	// partition related ddl
	return t == timodel.ActionAddTablePartition ||
		t == timodel.ActionExchangeTablePartition ||
		t == timodel.ActionReorganizePartition ||
		// reorg ddls
		t == timodel.ActionAddPrimaryKey ||
		t == timodel.ActionAddIndex ||
		t == timodel.ActionModifyColumn ||
		// following ddls can be fast when the downstream is TiDB, we must
		// still take them into consideration to ensure compatibility with all
		// MySQL-compatible databases.
		t == timodel.ActionAddColumn ||
		t == timodel.ActionAddColumns ||
		t == timodel.ActionDropColumn ||
		t == timodel.ActionDropColumns
}

func (m *DDLSink) execDDL(pctx context.Context, ddl *model.DDLEvent) error {
	ctx := pctx
	// When executing Reorg and Partition DDLs in TiDB, there is no timeout
	// mechanism by default. Instead, the system will wait for the DDL operation
	// to be executed or completed before proceeding.
	if !isReorgOrPartitionDDL(ddl.Type) {
		writeTimeout, _ := time.ParseDuration(m.cfg.WriteTimeout)
		writeTimeout += networkDriftDuration
		var cancelFunc func()
		ctx, cancelFunc = context.WithTimeout(pctx, writeTimeout)
		defer cancelFunc()
	}

	shouldSwitchDB := needSwitchDB(ddl)

	// Convert vector type to string type for unsupport database
	if m.needFormat {
		if newQuery := formatQuery(ddl.Query); newQuery != ddl.Query {
			log.Warn("format ddl query", zap.String("newQuery", newQuery), zap.String("query", ddl.Query), zap.String("collate", ddl.Collate), zap.String("charset", ddl.Charset))
			ddl.Query = newQuery
		}
	}

	failpoint.Inject("MySQLSinkExecDDLDelay", func() {
		select {
		case <-ctx.Done():
			failpoint.Return(ctx.Err())
		case <-time.After(time.Hour):
		}
		failpoint.Return(nil)
	})

	start := time.Now()
	log.Info("Start exec DDL", zap.String("namespace", m.id.Namespace), zap.String("changefeed", m.id.ID),
		zap.Uint64("commitTs", ddl.CommitTs), zap.String("DDL", ddl.Query))
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(ctx, "USE "+quotes.QuoteName(ddl.TableInfo.TableName.Schema)+";")
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback", zap.String("namespace", m.id.Namespace),
					zap.String("changefeed", m.id.ID), zap.Error(err))
			}
			return err
		}
	}

	// we try to set cdc write source for the ddl
	if err = pmysql.SetWriteSource(pctx, m.cfg, tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			if errors.Cause(rbErr) != context.Canceled {
				log.Error("Failed to rollback",
					zap.String("namespace", m.id.Namespace),
					zap.String("changefeed", m.id.ID), zap.Error(err))
			}
		}
		return err
	}

	if _, err = tx.ExecContext(ctx, ddl.Query); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback",
				zap.String("namespace", m.id.Namespace),
				zap.String("changefeed", m.id.ID),
				zap.String("sql", ddl.Query),
				zap.Error(err))
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		log.Error("Failed to exec DDL", zap.String("namespace", m.id.Namespace), zap.String("changefeed", m.id.ID),
			zap.Duration("duration", time.Since(start)), zap.String("sql", ddl.Query), zap.Error(err))
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Query info: %s; ", ddl.Query)))
	}

	log.Info("Exec DDL succeeded",
		zap.String("namespace", m.id.Namespace), zap.String("changefeed", m.id.ID),
		zap.Duration("duration", time.Since(start)), zap.String("sql", ddl.Query))
	return nil
}

// WriteCheckpointTs does nothing.
func (m *DDLSink) WriteCheckpointTs(_ context.Context, _ uint64, _ []*model.TableInfo) error {
	// Only for RowSink for now.
	return nil
}

// Close closes the database connection.
func (m *DDLSink) Close() {
	if m.statistics != nil {
		m.statistics.Close()
	}
	if m.db != nil {
		if err := m.db.Close(); err != nil {
			log.Warn("MySQL ddl sink close db wit error",
				zap.String("namespace", m.id.Namespace),
				zap.String("changefeed", m.id.ID),
				zap.Error(err))
		}
	}
}

func needSwitchDB(ddl *model.DDLEvent) bool {
	if len(ddl.TableInfo.TableName.Schema) == 0 {
		return false
	}
	if ddl.Type == timodel.ActionCreateSchema || ddl.Type == timodel.ActionDropSchema {
		return false
	}
	return true
}

// needFormatDDL checks vector type support
func needFormatDDL(db *sql.DB, cfg *pmysql.Config) bool {
	if !cfg.HasVectorType {
		log.Warn("please set `has-vector-type` to be true if a column is vector type when the downstream is not TiDB or TiDB version less than specify version",
			zap.Any("hasVectorType", cfg.HasVectorType), zap.Any("supportVectorVersion", defaultSupportVectorVersion))
		return false
	}
	versionInfo, err := export.SelectVersion(db)
	if err != nil {
		log.Warn("fail to get version", zap.Error(err), zap.Bool("isTiDB", cfg.IsTiDB))
		return false
	}
	serverInfo := version.ParseServerInfo(versionInfo)
	version := semver.New(defaultSupportVectorVersion)
	if !cfg.IsTiDB || serverInfo.ServerVersion.LessThan(*version) {
		log.Error("downstream unsupport vector type. it will be converted to longtext", zap.String("version", serverInfo.ServerVersion.String()), zap.String("supportVectorVersion", defaultSupportVectorVersion), zap.Bool("isTiDB", cfg.IsTiDB))
		return true
	}
	return false
}

func getDDLCreateTime(ctx context.Context, db *sql.DB) int64 {
	var ddlCreateTime int64 = -1 // default when scan failed
	row, err := db.QueryContext(ctx, "SELECT UNIX_TIMESTAMP()")
	if err != nil {
		log.Warn("selecting unix timestamp failed", zap.Error(err))
	} else {
		for row.Next() {
			err = row.Scan(&ddlCreateTime)
			if err != nil {
				log.Warn("getting ddlCreateTime failed", zap.Error(err))
			}
		}
		//nolint:sqlclosecheck
		_ = row.Close()
		_ = row.Err()
	}
	return ddlCreateTime
}

// getDDLStatusFromTiDB retrieves the synchronizing status of DDL from TiDB
func getDDLStatusFromTiDB(ctx context.Context, db *sql.DB, ddl string, createTime int64) (string, error) {
	linesOfRows := 10
	rowNum := linesOfRows
	rowOffset := 0
	queryMap := make(map[int]string)

	for {
		// every attempt try 10 history jobs
		showJobs := fmt.Sprintf("ADMIN SHOW DDL JOBS %d", rowNum)
		//nolint:rowserrcheck
		jobsRows, err := db.QueryContext(ctx, showJobs)
		if err != nil {
			return "", err
		}

		var jobsResults [][]string
		jobsResults, err = export.GetSpecifiedColumnValuesAndClose(jobsRows, "JOB_ID", "CREATE_TIME", "STATE")
		if err != nil {
			return "", err
		}

		for i := rowNum - linesOfRows; i < rowNum && i < len(jobsResults); i++ {
			ddlCreateTimeStr := jobsResults[i][1]
			var ddlCreateTimeParse time.Time
			ddlCreateTimeParse, err = time.Parse("2006-01-02 15:04:05", ddlCreateTimeStr)
			if err != nil {
				return "", err
			}
			ddlCreateTime := ddlCreateTimeParse.Unix()

			// ddlCreateTime and createTime are both based on timezone of downstream
			if ddlCreateTime >= createTime {
				var jobID int
				jobID, err = strconv.Atoi(jobsResults[i][0])
				if err != nil {
					return "", err
				}

				for {
					ddlQuery, ok := queryMap[jobID]
					if !ok {
						// jobID does not exist, expand queryMap for deeper search
						showJobsLimitNext := fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET %d", rowOffset)
						var rowsLimitNext *sql.Rows
						//nolint:rowserrcheck
						rowsLimitNext, err = db.QueryContext(ctx, showJobsLimitNext)
						if err != nil {
							return "", err
						}

						var resultsLimitNext [][]string
						resultsLimitNext, err = export.GetSpecifiedColumnValuesAndClose(rowsLimitNext, "JOB_ID", "QUERY")
						if err != nil {
							return "", err
						}
						if len(resultsLimitNext) == 0 {
							// JOB QUERIES has been used up
							// requested DDL cannot be found
							return "", nil
						}

						// if new DDLs are written to TiDB after the last query 'ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET'
						// we may get duplicate rows here, but it does not affect the checking
						for k := range resultsLimitNext {
							var jobIDForLimit int
							jobIDForLimit, err = strconv.Atoi(resultsLimitNext[k][0])
							if err != nil {
								return "", err
							}
							queryMap[jobIDForLimit] = resultsLimitNext[k][1]
						}
						rowOffset += linesOfRows
					} else {
						if ddl == ddlQuery {
							return jobsResults[i][2], nil
						}
						break
					}
				}
			} else {
				// ddlCreateTime is monotonous in jobsResults
				// requested DDL cannot be found
				return "", nil
			}
		}
		if len(jobsResults) == rowNum {
			rowNum += linesOfRows
		} else {
			// jobsResults has been checked thoroughly
			return "", nil
		}
	}
}
