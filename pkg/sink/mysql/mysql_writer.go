// Copyright 2024 PingCAP, Inc.
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
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/util"
)

const (
	defaultDDLMaxRetry uint64 = 20

	// networkDriftDuration is used to construct a context timeout for database operations.
	networkDriftDuration = 5 * time.Second

	defaultSupportVectorVersion = "8.4.0"
)

// MysqlWriter is responsible for writing various dml events, ddl events, syncpoint events to mysql downstream.
type MysqlWriter struct {
	ctx          context.Context
	db           *sql.DB
	cfg          *MysqlConfig
	ChangefeedID common.ChangeFeedID

	syncPointTableInit     bool
	lastCleanSyncPointTime time.Time

	ddlTsTableInit   bool
	tableSchemaStore *util.TableSchemaStore

	// asyncDDLState is used to store the state of async ddl.
	// key: tableID, value: state(0: unknown state , 1: executing, 2: no executing ddl)
	asyncDDLState sync.Map

	// implement stmtCache to improve performance, especially when the downstream is TiDB
	stmtCache *lru.Cache
	// Indicate if the CachePrepStmts should be enabled or not
	cachePrepStmts   bool
	maxAllowedPacket int64

	statistics *metrics.Statistics
	needFormat bool
}

func NewMysqlWriter(
	ctx context.Context,
	db *sql.DB,
	cfg *MysqlConfig,
	changefeedID common.ChangeFeedID,
	statistics *metrics.Statistics,
	needFormatVectorType bool,
) *MysqlWriter {
	return &MysqlWriter{
		ctx:                    ctx,
		db:                     db,
		cfg:                    cfg,
		syncPointTableInit:     false,
		ChangefeedID:           changefeedID,
		lastCleanSyncPointTime: time.Now(),
		ddlTsTableInit:         false,
		asyncDDLState:          sync.Map{},
		cachePrepStmts:         cfg.CachePrepStmts,
		maxAllowedPacket:       cfg.MaxAllowedPacket,
		stmtCache:              cfg.stmtCache,
		statistics:             statistics,
		needFormat:             needFormatVectorType,
	}
}

func (w *MysqlWriter) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	w.tableSchemaStore = tableSchemaStore
}

func (w *MysqlWriter) FlushDDLEvent(event *commonEvent.DDLEvent) error {
	if w.cfg.IsTiDB {
		// first we check whether there is some async ddl executed now.
		w.waitAsyncDDLDone(event)
	}

	// check the ddl should by async or sync executed.
	if needAsyncExecDDL(event.GetDDLType()) && w.cfg.IsTiDB {
		// for async exec ddl, we don't flush ddl ts here. Because they don't block checkpointTs.
		err := w.asyncExecAddIndexDDLIfTimeout(event)
		if err != nil {
			return errors.Trace(err)
		}
	} else if !(event.TiDBOnly && !w.cfg.IsTiDB) {
		err := w.execDDLWithMaxRetries(event)
		if err != nil {
			return errors.Trace(err)
		}

		// We need to record ddl' ts after each ddl for each table in the downstream when sink is mysql-compatible.
		// Only in this way, when the node restart, we can continue sync data from the last ddl ts at least.
		// Otherwise, after restarting, we may sync old data in new schema, which will leading to data loss.

		// We make Flush ddl ts before callback(), in order to make sure the ddl ts is flushed
		// before new checkpointTs will report to maintainer. Therefore, when the table checkpointTs is forward,
		// we can ensure the ddl and ddl ts are both flushed downstream successfully.
		err = w.FlushDDLTs(event)
		if err != nil {
			return err
		}
	}

	for _, callback := range event.PostTxnFlushed {
		callback()
	}
	return nil
}

func (w *MysqlWriter) Flush(events []*commonEvent.DMLEvent) error {
	dmls, err := w.prepareDMLs(events)
	if err != nil {
		return errors.Trace(err)
	}
	defer dmlsPool.Put(dmls) // Return dmls to pool after use

	if dmls.rowCount == 0 {
		return nil
	}

	if !w.cfg.DryRun {
		if err = w.execDMLWithMaxRetries(dmls); err != nil {
			return errors.Trace(err)
		}
	} else {
		if err = w.statistics.RecordBatchExecution(func() (int, int64, error) {
			return dmls.rowCount, dmls.approximateSize, nil
		}); err != nil {
			return errors.Trace(err)
		}
	}

	for _, event := range events {
		for _, callback := range event.PostTxnFlushed {
			callback()
		}
	}
	return nil
}

func (w *MysqlWriter) Close() {
	if w.stmtCache != nil {
		w.stmtCache.Purge()
	}
}
