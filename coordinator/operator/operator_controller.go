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

package operator

import (
	"container/heap"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/coordinator/changefeed"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// Controller is the operator controller, it manages all operators.
// And the Controller is responsible for the execution of the operator.
type Controller struct {
	changefeedDB  *changefeed.ChangefeedDB
	operators     map[model.ChangeFeedID]Operator
	runningQueue  operatorQueue
	batchSize     int
	messageCenter messaging.MessageCenter

	lock sync.RWMutex
}

func NewOperatorController(mc messaging.MessageCenter,
	db *changefeed.ChangefeedDB,
	batchSize int) *Controller {
	oc := &Controller{
		operators:     make(map[model.ChangeFeedID]Operator),
		runningQueue:  make(operatorQueue, 0),
		messageCenter: mc,
		batchSize:     batchSize,
		changefeedDB:  db,
	}
	return oc
}

// Execute periodically execute the operator
// todo: use a better way to control the execution frequency
func (oc *Controller) Execute() time.Time {
	executedItem := 0
	for {
		r, next := oc.pollQueueingOperator()
		if !next {
			return time.Now().Add(time.Millisecond * 200)
		}
		if r == nil {
			continue
		}

		oc.lock.RLock()
		msg := r.Schedule()
		oc.lock.RUnlock()

		if msg != nil {
			_ = oc.messageCenter.SendCommand(msg)
			log.Info("send command to maintainer",
				zap.String("operator", r.String()))
		}
		executedItem++
		if executedItem >= oc.batchSize {
			return time.Now().Add(time.Millisecond * 50)
		}
	}
}

// AddOperator adds an operator to the controller, if the operator already exists, return false.
func (oc *Controller) AddOperator(op Operator) bool {
	oc.lock.Lock()
	defer oc.lock.Unlock()

	if _, ok := oc.operators[op.ID()]; ok {
		log.Info("add operator failed, operator already exists",
			zap.String("operator", op.String()))
		return false
	}
	cf := oc.changefeedDB.GetByID(op.ID())
	if cf == nil {
		log.Warn("add operator failed, changefeed not found",
			zap.String("operator", op.String()))
		return false
	}
	oc.pushOperator(op)
	return true
}

// StopChangefeed stop changefeed when the changefeed is stopped/removed.
// if remove is true, it will remove the changefeed from the chagnefeed DB
// if remove is false, it only marks as the changefeed stooped in changefeed DB, so we will not schedule the changefeed again
func (oc *Controller) StopChangefeed(cfID model.ChangeFeedID, remove bool) {
	oc.lock.Lock()
	defer oc.lock.Unlock()

	var cf *changefeed.Changefeed
	if remove {
		cf = oc.changefeedDB.RemoveByChangefeedID(cfID)
	} else {
		cf = oc.changefeedDB.StopByChangefeedID(cfID)
	}
	if cf == nil {
		log.Info("changefeed is not scheduled")
		return
	}
	if old, ok := oc.operators[cfID]; ok {
		log.Info("changefeed is stopped , replace the old one",
			zap.String("changefeed", cfID.ID),
			zap.String("operator", old.String()))
		old.OnTaskRemoved()
		delete(oc.operators, old.ID())
	}
	op := NewRemoveChangefeedOperator(cf)
	oc.pushOperator(op)
}

func (oc *Controller) UpdateOperatorStatus(id model.ChangeFeedID, from node.ID,
	status *heartbeatpb.MaintainerStatus) {
	oc.lock.RLock()
	defer oc.lock.RUnlock()

	op, ok := oc.operators[id]
	if ok {
		op.Check(from, status)
	}
}

// OnNodeRemoved is called when a node is offline,
// the controller will mark all maintainers on the node as absent if no operator is handling it,
// then the controller will notify all operators.
func (oc *Controller) OnNodeRemoved(n node.ID) {
	oc.lock.RLock()
	defer oc.lock.RUnlock()

	for _, cf := range oc.changefeedDB.GetByNodeID(n) {
		_, ok := oc.operators[cf.ID]
		if !ok {
			oc.changefeedDB.MarkMaintainerAbsent(cf)
		}
	}
	for _, op := range oc.operators {
		op.OnNodeRemove(n)
	}
}

// GetOperator returns the operator by id.
func (oc *Controller) GetOperator(id model.ChangeFeedID) Operator {
	oc.lock.RLock()
	defer oc.lock.RUnlock()
	return oc.operators[id]
}

// OperatorSize returns the number of operators in the controller.
func (oc *Controller) OperatorSize() int {
	oc.lock.RLock()
	defer oc.lock.RUnlock()
	return len(oc.operators)
}

// pollQueueingOperator returns the operator need to be executed,
// "next" is true to indicate that it may exist in next attempt,
// and false is the end for the poll.
func (oc *Controller) pollQueueingOperator() (Operator, bool) {
	oc.lock.Lock()
	defer oc.lock.Unlock()
	if oc.runningQueue.Len() == 0 {
		return nil, false
	}
	item := heap.Pop(&oc.runningQueue).(*operatorWithTime)
	op := item.op
	opID := item.op.ID()
	// always call the PostFinish method to ensure the operator is cleaned up by itself.
	if op.IsFinished() {
		op.PostFinish()
		delete(oc.operators, opID)
		metrics.CoordinatorFinishedOperatorCount.WithLabelValues(op.Type()).Inc()
		metrics.CoordinatorOperatorDuration.WithLabelValues(op.Type()).Observe(time.Since(item.enqueueTime).Seconds())
		log.Info("operator finished",
			zap.String("operator", opID.String()),
			zap.String("operator", op.String()))
		return nil, true
	}
	now := time.Now()
	if now.Before(item.time) {
		heap.Push(&oc.runningQueue, item)
		return nil, false
	}
	// pushes with new notify time.
	item.time = time.Now().Add(time.Millisecond * 500)
	heap.Push(&oc.runningQueue, item)
	return op, true
}

// pushOperator add an operator to the controller queue.
func (oc *Controller) pushOperator(op Operator) {
	log.Info("add operator to running queue",
		zap.String("operator", op.String()))
	oc.operators[op.ID()] = op
	op.Start()
	heap.Push(&oc.runningQueue, &operatorWithTime{op: op, time: time.Now(), enqueueTime: time.Now()})
	metrics.CoordinatorCreatedOperatorCount.WithLabelValues(op.Type()).Inc()
}
