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
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap/log"
	"go.uber.org/zap"
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
	// balanceRegionRetryLimit is the limit to retry schedule for a source store.
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

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	stores := cluster.GetStores()
	filters := []filter.Filter{filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true}}
	sort.Slice(stores, func(i, j int) bool {
		iSize := stores[i].GetRegionSize()
		jSize := stores[j].GetRegionSize()
		// An offline store cannot be a source, but it can still receive peers,
		// so give it the lowest score.
		if stores[i].IsOffline() {
			iSize = 0
		}
		if stores[j].IsOffline() {
			jSize = 0
		}
		return iSize > jSize
	})

	for _, source := range stores {
		if filter.Source(cluster, source, filters) {
			continue
		}
		sourceSize := source.GetRegionSize()
		// Candidate targets: healthy stores with fewer regions, smallest first.
		targets := make([]*core.StoreInfo, 0, len(stores))
		for i := len(stores) - 1; i >= 0; i-- {
			target := stores[i]
			if target.GetID() == source.GetID() {
				continue
			}
			if filter.Target(cluster, target, filters) {
				continue
			}
			if sourceSize <= target.GetRegionSize() {
				continue
			}
			targets = append(targets, target)
		}
		if len(targets) == 0 {
			continue
		}

		for i := 0; i < balanceRegionRetryLimit; i++ {
			region := s.findRegion(cluster, source.GetID())
			if region == nil {
				break
			}
			if op := s.createOperator(cluster, source, region, targets); op != nil {
				return op
			}
		}
		log.Debug("no operator created for selected store", zap.String("scheduler", s.GetName()), zap.Uint64("source", source.GetID()))
	}
	return nil
}

// findRegion picks a region that has a peer on the given store, preferring
// regions with pending peers so that they are repaired first.
func (s *balanceRegionScheduler) findRegion(cluster opt.Cluster, storeID uint64) *core.RegionInfo {
	if region := cluster.RandPendingRegion(storeID, core.HealthRegionAllowPending()); region != nil {
		return region
	}
	if region := cluster.RandFollowerRegion(storeID, core.HealthRegion()); region != nil {
		return region
	}
	return cluster.RandLeaderRegion(storeID, core.HealthRegion())
}

// createOperator moves a peer of the region from the source store to one of the
// target stores.
func (s *balanceRegionScheduler) createOperator(cluster opt.Cluster, source *core.StoreInfo,
	region *core.RegionInfo, targets []*core.StoreInfo) *operator.Operator {
	if len(region.GetPeers()) < cluster.GetMaxReplicas() {
		// The region is under replicated; replicas will add peers for it.
		return nil
	}
	sourceID := source.GetID()
	for _, target := range targets {
		targetID := target.GetID()
		if region.GetStorePeer(targetID) != nil {
			continue
		}
		peer, err := cluster.AllocPeer(targetID)
		if err != nil {
			log.Debug("alloc peer failed", zap.String("scheduler", s.GetName()), zap.Uint64("target", targetID), zap.Error(err))
			continue
		}
		op, err := operator.CreateMovePeerOperator("balance-region", cluster, region,
			operator.OpBalance, sourceID, targetID, peer.GetId())
		if err != nil {
			log.Debug("create operator failed", zap.String("scheduler", s.GetName()), zap.Error(err))
			continue
		}
		return op
	}
	return nil
}
