// Copyright 2017 PingCAP, Inc.
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

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func generateOperatorMoveRegionToSmallerStore(smaller, larger *core.StoreInfo, region *core.RegionInfo, cluster opt.Cluster) *operator.Operator {
	newPeer, err := cluster.AllocPeer(smaller.GetID())
	if err != nil {
		log.Panicf("failed to alloc peer")
	}
	op, err := operator.CreateMovePeerOperator("3c test", cluster, region, operator.OpBalance, larger.GetID(), smaller.GetID(), newPeer.GetId())
	if err != nil {
		log.Panicf("failed to alloc peer")
	}
	return op
}

func findRegionMoveOut(storeId uint64, cluster opt.Cluster) *core.RegionInfo {
	pending := cluster.RandPendingRegion(storeId)
	if pending != nil {
		return pending
	}

	follower := cluster.RandFollowerRegion(storeId)
	if follower != nil {
		return follower
	}
	leader := cluster.RandLeaderRegion(storeId)
	if leader != nil {
		return leader
	}
	return nil
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	stores := cluster.GetStores()

	// sort the stores by region size
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() < stores[j].GetRegionSize()
	})

	sourceStoreIndex, regionMoveOut := findSourceStoreAndRegion(stores, cluster)
	if regionMoveOut == nil {
		return nil
	}

	targetStoreIndex, existMoveInStore := findTargetStore(stores, sourceStoreIndex, regionMoveOut, cluster)
	if !existMoveInStore {
		return nil
	}

	return generateOperatorMoveRegionToSmallerStore(stores[targetStoreIndex], stores[sourceStoreIndex], regionMoveOut, cluster)
}

func findSourceStoreAndRegion(stores []*core.StoreInfo, cluster opt.Cluster) (int, *core.RegionInfo) {
	var sourceStoreIndex int
	var regionMoveOut *core.RegionInfo
	for i := len(stores) - 1; i >= 1; i-- {
		region := findRegionMoveOut(stores[i].GetID(), cluster)
		if region != nil {
			sourceStoreIndex = i
			regionMoveOut = region
			break
		}
	}
	return sourceStoreIndex, regionMoveOut
}

func findTargetStore(stores []*core.StoreInfo, sourceStoreIndex int, regionMoveOut *core.RegionInfo, cluster opt.Cluster) (int, bool) {
	var targetStoreIndex int
	var existMoveInStore bool
	for i := 0; i < len(stores)-1; i++ {
		if stores[i].IsOffline() || stores[i].IsUnhealth() || len(regionMoveOut.GetPeers()) < cluster.GetMaxReplicas() {
			continue
		}
		meta := regionMoveOut.GetMeta()
		valid := true
		for _, p := range meta.Peers {
			if p.StoreId == stores[i].GetID() {
				valid = false
				break
			}
		}
		if valid {
			targetStoreIndex = i
			existMoveInStore = true
			break
		}
	}
	return targetStoreIndex, existMoveInStore
}
