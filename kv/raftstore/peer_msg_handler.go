package raftstore

import (
	"bytes"
	"fmt"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
	"sort"
	"strconv"
	"strings"
	"time"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) sendRaftMessage(msg pb.Message) error {
	currentRegion := d.Region()
	raftMsg := rspb.RaftMessage{
		RegionId:    d.regionId,
		FromPeer:    d.peer.Meta,
		ToPeer:      d.getPeerFromCache(msg.To),
		Message:     &msg,
		RegionEpoch: currentRegion.GetRegionEpoch(),
		StartKey:    currentRegion.GetStartKey(),
		EndKey:      currentRegion.GetEndKey(),
	}
	//log.Infof("%s send raft message %+v", d.Tag, raftMsg)
	if msg.MsgType == pb.MessageType_MsgHeartbeat {
		//log.Infof("%s send heart to (id %d, store %d) ", d.Tag, msg.To, d.peerCache[msg.To].StoreId)
	}
	err := d.ctx.trans.Send(&raftMsg)
	return err
}

func (d *peerMsgHandler) findProposal(entry pb.Entry, delete bool) (*proposal, bool) {
	// TODO: this find function could with time complexity o(1)
	if delete {
		for len(d.proposals) > 0 && d.proposals[0].index <= entry.Index {
			prop := d.proposals[0]
			d.proposals = d.proposals[1:]
			if prop.term == entry.Term && prop.index == entry.Index {
				return prop, false
			}
		}
	} else {
		for _, prop := range d.proposals {
			if prop.term == entry.Term && prop.index == entry.Index {
				return prop, false
			}
		}
	}

	return nil, true
}

func (d *peerMsgHandler) executeGetRequest(get *raft_cmdpb.GetRequest) (*raft_cmdpb.GetResponse, error) {
	kvStore := d.peerStorage.Engines.Kv
	value, err := engine_util.GetCF(kvStore, get.GetCf(), get.GetKey())
	if err != nil {
		return nil, err
	}
	return &raft_cmdpb.GetResponse{Value: value}, nil
}

func (d *peerMsgHandler) executePutRequest(put *raft_cmdpb.PutRequest, kvWB *engine_util.WriteBatch) (*raft_cmdpb.PutResponse, error) {
	kvWB.SetCF(put.GetCf(), put.GetKey(), put.GetValue())
	return &raft_cmdpb.PutResponse{}, nil
}

func (d *peerMsgHandler) executeDeleteRequest(delete *raft_cmdpb.DeleteRequest, kvWB *engine_util.WriteBatch) (*raft_cmdpb.DeleteResponse, error) {
	kvWB.DeleteCF(delete.GetCf(), delete.GetKey())
	return &raft_cmdpb.DeleteResponse{}, nil
}

func (d *peerMsgHandler) executeSnapRequest(getSnap *raft_cmdpb.SnapRequest) (*raft_cmdpb.SnapResponse, error) {

	return &raft_cmdpb.SnapResponse{Region: d.Region()}, nil
}

func (d *peerMsgHandler) applyNormalRaftCommand(entry pb.Entry, raftCmd *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	var responses []*raft_cmdpb.Response

	curRegion := d.Region()

	for _, req := range raftCmd.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			get := req.GetGet()
			if !engine_util.WithinRange(get.GetKey(), curRegion.GetStartKey(), curRegion.GetEndKey()) {
				err := &errorpb.KeyNotInRegion{
					Key:      get.GetKey(),
					RegionId: curRegion.GetId(),
					StartKey: curRegion.GetStartKey(),
					EndKey:   curRegion.GetEndKey(),
				}
				return &raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{
						Error: &errorpb.Error{
							Message:        err.String(),
							KeyNotInRegion: err,
						},
					},
					Responses: responses,
				}
			}
		case raft_cmdpb.CmdType_Put:
			put := req.GetPut()
			if !engine_util.WithinRange(put.GetKey(), curRegion.GetStartKey(), curRegion.GetEndKey()) {
				err := &errorpb.KeyNotInRegion{
					Key:      put.GetKey(),
					RegionId: curRegion.GetId(),
					StartKey: curRegion.GetStartKey(),
					EndKey:   curRegion.GetEndKey(),
				}
				return &raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{
						Error: &errorpb.Error{
							Message:        err.String(),
							KeyNotInRegion: err,
						},
					},
					Responses: responses,
				}
			}
		case raft_cmdpb.CmdType_Delete:
			del := req.GetDelete()
			if !engine_util.WithinRange(del.GetKey(), curRegion.GetStartKey(), curRegion.GetEndKey()) {
				err := &errorpb.KeyNotInRegion{
					Key:      del.GetKey(),
					RegionId: curRegion.GetId(),
					StartKey: curRegion.GetStartKey(),
					EndKey:   curRegion.GetEndKey(),
				}
				return &raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{
						Error: &errorpb.Error{
							Message:        err.String(),
							KeyNotInRegion: err,
						},
					},
					Responses: responses,
				}
			}
		}
	}

	for _, req := range raftCmd.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			resp, err := d.executeGetRequest(req.GetGet())
			if err != nil {
				// TODO: handle this error.
				responses = append(responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get:     resp,
				})
			} else {
				responses = append(responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get:     resp,
				})
			}
		case raft_cmdpb.CmdType_Put:
			resp, err := d.executePutRequest(req.GetPut(), kvWB)
			if err != nil {
				// TODO: handle this error.
				responses = append(responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     resp,
				})
			} else {
				responses = append(responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     resp,
				})
			}
		case raft_cmdpb.CmdType_Delete:
			resp, err := d.executeDeleteRequest(req.GetDelete(), kvWB)
			if err != nil {
				// TODO: handle this error.
				responses = append(responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  resp,
				})
			} else {
				responses = append(responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  resp,
				})
			}
		case raft_cmdpb.CmdType_Snap:
			resp, err := d.executeSnapRequest(req.GetSnap())
			if err != nil {
				// TODO: handle this error.
				responses = append(responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap:    resp,
				})
			} else {
				prop, notFound := d.findProposal(entry, false)
				if !notFound {
					prop.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
				}

				responses = append(responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap:    resp,
				})
			}

		}
	}
	return &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{Error: nil},
		Responses: responses,
	}
}

func (d *peerMsgHandler) applyAdminRaftCommand(entry pb.Entry, adminRequest *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	if adminRequest.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_CompactLog {
		// modify RaftTruncatedState in RaftApplyState.
		// schedule a task to raftlog-gc work by ScheduleCompactLog.

		gcTask := runner.RaftLogGCTask{
			RaftEngine: d.peerStorage.Engines.Raft,
			RegionID:   d.regionId,
			StartIdx:   d.peerStorage.applyState.TruncatedState.Index,
			EndIdx:     adminRequest.AdminRequest.CompactLog.CompactIndex,
		}

		d.peerStorage.applyState.TruncatedState.Index = adminRequest.AdminRequest.CompactLog.CompactIndex
		d.peerStorage.applyState.TruncatedState.Term = adminRequest.AdminRequest.CompactLog.CompactTerm
		d.LastCompactedIdx = adminRequest.AdminRequest.CompactLog.CompactIndex
		if err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
			log.Panicf("%s failed to compactLog, detail %+v", d.Tag, d.peerStorage.applyState)
		}
		d.peerStorage.regionSched <- gcTask

		return &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
				CompactLog: &raft_cmdpb.CompactLogResponse{},
			},
		}

	}
	// TODO: this code will never be called , remove it.
	if adminRequest.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_TransferLeader {

		if d.IsLeader() {
			log.Errorf("transfer leader called by node %d", d.PeerId())
			d.RaftGroup.TransferLeader(adminRequest.AdminRequest.TransferLeader.Peer.Id)
		}

		return &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			},
		}
	}

	if adminRequest.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_Split {
		curRegion := d.Region()

		splitRequest := adminRequest.AdminRequest.Split
		if bytes.Equal(splitRequest.GetSplitKey(), curRegion.GetEndKey()) {
			regions := []*metapb.Region{}
			d.ctx.storeMeta.Lock()
			for _, r := range d.ctx.storeMeta.regions {
				regions = append(regions, r)
			}
			d.ctx.storeMeta.Unlock()
			return &raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{
					Error: &errorpb.Error{
						Message:      "",
						StaleCommand: &errorpb.StaleCommand{},
					},
				},
			}
		}

		curRegion.RegionEpoch.Version++
		curRegion.RegionEpoch.ConfVer = InitEpochConfVer
		newRegion := cloneRegion(curRegion)

		newRegion.StartKey = splitRequest.SplitKey
		newRegion.EndKey = curRegion.EndKey
		newRegion.Id = splitRequest.NewRegionId
		newRegion.RegionEpoch.ConfVer = InitEpochConfVer
		newRegion.RegionEpoch.Version = curRegion.RegionEpoch.Version

		for i := range splitRequest.NewPeerIds {
			newRegion.Peers[i].Id = splitRequest.NewPeerIds[i]
		}

		curRegion.EndKey = splitRequest.SplitKey
		//log.Errorf("%s split request peers: %+v, new curRegion peers: %+v", d.Tag, splitRequest.NewPeerIds, newRegion.Peers)

		regionLocalState := new(rspb.RegionLocalState)
		regionLocalState.State = rspb.PeerState_Normal
		regionLocalState.Region = d.Region()
		err := kvWB.SetMeta(meta.RegionStateKey(d.regionId), regionLocalState)
		if err != nil {
			log.Panicf("%s failed to split curRegion, error: %+v", d.Tag, err)
		}

		newRegionLocalState := new(rspb.RegionLocalState)
		newRegionLocalState.State = rspb.PeerState_Normal
		newRegionLocalState.Region = newRegion
		err = kvWB.SetMeta(meta.RegionStateKey(newRegion.Id), newRegionLocalState)
		if err != nil {
			log.Panicf("%s failed to split curRegion, error: %+v", d.Tag, err)
		}
		kvWB.MustWriteToDB(d.ctx.engine.Kv)

		regions := []*metapb.Region{}
		d.ctx.storeMeta.Lock()
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: curRegion})
		d.ctx.storeMeta.regions[d.regionId] = curRegion
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
		d.ctx.storeMeta.regions[newRegion.Id] = newRegion
		for _, r := range d.ctx.storeMeta.regions {
			regions = append(regions, r)
		}
		d.ctx.storeMeta.Unlock()

		newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			log.Panicf("%d  failed to create curRegion %d", d.storeID(), newRegion.Id)
		}

		for _, p := range newRegion.Peers {
			newPeer.insertPeerCache(p)
		}

		d.ctx.router.register(newPeer)
		err = d.ctx.router.send(newRegion.Id, message.Msg{RegionID: newRegion.Id, Type: message.MsgTypeStart})
		if err != nil {
			log.Panicf("%d failed to create curRegion %d ", d.storeID(), newRegion.Id)
		}
		kvWB.Reset()
		d.peerStorage.clearExtraData(curRegion)
		if d.IsLeader() {
			less := func(i, j int) bool {
				// 按照 startkey 进行比较
				startKeyComparison := strings.Compare(string(regions[i].StartKey), string(regions[j].StartKey))
				if startKeyComparison != 0 {
					return startKeyComparison < 0
				}
				// 如果 startkey 相同，则按照 endkey 进行比较
				return strings.Compare(string(regions[i].EndKey), string(regions[j].EndKey)) < 0
			}

			// 使用 sort.Slice 进行排序
			sort.Slice(regions, less)
			//log.Errorf("%s split finished, regions: %+v", d.Tag, regions)
		}
		return &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_Split,
				Split: &raft_cmdpb.SplitResponse{
					Regions: regions,
				},
			},
		}

	}
	log.Panicf("unimplemented raft admin command")
	return nil
}

func (d *peerMsgHandler) applyAddNodeConfChangeRaftCommand(entry *pb.Entry, change *pb.ConfChange, changePeer *raft_cmdpb.ChangePeerRequest, kvWB *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {

	//if !d.peer.AnyNewPeerCatchUp(change.NodeId) && d.IsLeader() {
	//	kvWB.Reset()
	//	return ErrResp(errors.New("failed to apply conf change, because new peer not catch up yet."))
	//}

	d.peerStorage.region.RegionEpoch.ConfVer++
	newPeer := changePeer.Peer
	d.peerStorage.region.Peers = append(d.peerStorage.region.Peers, newPeer)
	d.insertPeerCache(newPeer)
	regionLocalState := new(rspb.RegionLocalState)
	regionLocalState.State = rspb.PeerState_Normal
	regionLocalState.Region = d.Region()
	clone := cloneRegion(d.Region())

	d.ctx.storeMeta.RWMutex.Lock()
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: clone})
	d.ctx.storeMeta.regions[d.regionId] = clone
	d.ctx.storeMeta.RWMutex.Unlock()
	// TODO: after peer storage update, clear extra data.
	//d.peerStorage.clearMeta(kvWB, nil)
	err := kvWB.SetMeta(meta.RegionStateKey(d.regionId), regionLocalState)
	if err != nil {
		log.Errorf("failed to set region local state, err: %+v", err)
	}

	return &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: d.Region()},
		},
	}
}

func cloneRegion(region *metapb.Region) *metapb.Region {
	clone := new(metapb.Region)
	clone.RegionEpoch = new(metapb.RegionEpoch)
	clone.RegionEpoch.ConfVer = region.RegionEpoch.ConfVer
	clone.RegionEpoch.Version = region.RegionEpoch.Version

	clone.Id = region.Id
	clone.StartKey = region.StartKey
	clone.EndKey = region.EndKey
	for _, p := range region.Peers {
		clone.Peers = append(clone.Peers, &metapb.Peer{Id: p.Id, StoreId: p.StoreId})
	}
	return clone
}

func (d *peerMsgHandler) applyRemoveOtherNodeConfChange(entry *pb.Entry, change *pb.ConfChange, kvWB *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {

	d.peerStorage.region.RegionEpoch.ConfVer++
	var newPeers []*metapb.Peer
	for _, peerNode := range d.peerStorage.region.Peers {
		if peerNode.Id != change.NodeId {
			newPeers = append(newPeers, peerNode)
		}
	}
	d.peerStorage.region.Peers = newPeers
	clone := cloneRegion(d.Region())

	d.ctx.storeMeta.RWMutex.Lock()
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: clone})
	d.ctx.storeMeta.regions[d.regionId] = clone
	d.ctx.storeMeta.RWMutex.Unlock()

	regionLocalState := new(rspb.RegionLocalState)
	regionLocalState.State = rspb.PeerState_Normal
	regionLocalState.Region = d.Region()
	err := kvWB.SetMeta(meta.RegionStateKey(d.regionId), regionLocalState)
	if err != nil {
		log.Errorf("failed to set region local state, err: %+v", err)
	}

	// destroy current node if it was removed.

	d.peer.removePeerCache(change.NodeId)

	return &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: d.Region()},
		},
	}
}

func (d *peerMsgHandler) applyRemoveNodeConfChangeRaftCommand(entry *pb.Entry, change *pb.ConfChange, kvWB *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	if change.NodeId != d.PeerId() {
		return d.applyRemoveOtherNodeConfChange(entry, change, kvWB)
	}
	d.peerStorage.region.RegionEpoch.ConfVer++
	var newPeers []*metapb.Peer
	for _, peerNode := range d.peerStorage.region.Peers {
		if peerNode.Id != change.NodeId {
			newPeers = append(newPeers, peerNode)
		}
	}
	d.peerStorage.region.Peers = newPeers
	clone := cloneRegion(d.Region())

	d.ctx.storeMeta.RWMutex.Lock()
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: clone})
	d.ctx.storeMeta.regions[d.regionId] = clone
	d.ctx.storeMeta.RWMutex.Unlock()

	regionLocalState := new(rspb.RegionLocalState)
	isTombstone := true
	for _, newPeer := range newPeers {
		if newPeer.StoreId == d.storeID() {
			isTombstone = false
		}
	}
	if isTombstone {
		regionLocalState.State = rspb.PeerState_Tombstone
	} else {
		regionLocalState.State = rspb.PeerState_Normal
	}
	regionLocalState.Region = d.Region()
	err := kvWB.SetMeta(meta.RegionStateKey(d.regionId), regionLocalState)
	if err != nil {
		log.Errorf("failed to set region local state, err: %+v", err)
	}

	// destroy current node if it was removed.
	d.peer.removePeerCache(change.NodeId)
	kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	d.peer.stopped = true
	kvWB.Reset()
	d.destroyPeer()
	return nil
}

func (d *peerMsgHandler) applyConfChangeRaftCommand(entry pb.Entry, change pb.ConfChange, kvWB *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	//log.Infof("%d is applying admin raft command index: %d, term %d, command type: %+v", d.PeerId(), entry.Index, entry.Term, change.ChangeType)
	var resp *raft_cmdpb.RaftCmdResponse

	//log.Infof("conf change details: %+v", change)
	//log.Errorf("%s change region epoch confversion to %d, entry index: %d", d.Tag, d.peerStorage.region.RegionEpoch.ConfVer, entry.Index)

	var confChangeRequest raft_cmdpb.RaftCmdRequest
	err := proto.Unmarshal(change.Context, &confChangeRequest)
	if err != nil {
		log.Panicf("%s failed to apply conf change command, err: %+v", d.Tag, err)
	}
	if err := util.CheckRegionEpoch(&confChangeRequest, d.Region(), true); err != nil {
		return ErrResp(err)
	}

	if change.ChangeType == pb.ConfChangeType_AddNode {
		resp = d.applyAddNodeConfChangeRaftCommand(&entry, &change, confChangeRequest.AdminRequest.ChangePeer, kvWB)
	}
	if change.ChangeType == pb.ConfChangeType_RemoveNode {
		resp = d.applyRemoveNodeConfChangeRaftCommand(&entry, &change, kvWB)
	}
	log.Infof("%s apply conf change, region: %+v", d.Tag, d.Region())
	d.RaftGroup.ApplyConfChange(
		pb.ConfChange{
			ChangeType: change.ChangeType,
			NodeId:     change.NodeId,
		})
	return resp
}

func (d *peerMsgHandler) mayExecuteDestroyPeer(entry *pb.Entry, change *pb.ConfChange) bool {
	if d.PeerId() != change.NodeId {
		return false
	}
	//if d.RaftGroup.Raft.RaftLog.LastIndex() != entry.Index {
	//	log.Errorf("%d reject to destroy, because it was restarted node. lastIndex %d, conf change index %d", d.PeerId(), d.RaftGroup.Raft.RaftLog.LastIndex(), entry.Index)
	//	return false
	//}
	return true
}

func (d *peerMsgHandler) applyRaftCommand(entry pb.Entry, kvWB *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	//log.Infof("%d is applying command index: %d, term: %d", d.PeerId(), entry.Index, entry.Term)

	if entry.EntryType == pb.EntryType_EntryNormal {
		// apply normal requests
		if entry.Data == nil {
			return ErrResp(errors.New("empty entry"))
		}
		var raftCmd raft_cmdpb.RaftCmdRequest

		if err := proto.Unmarshal(entry.Data, &raftCmd); err != nil {
			log.Errorf("%d failed to apply raft command index: %d, term: %d, err:%+v", d.PeerId(), entry.Index, entry.Term, err)
			return ErrResp(err)
		}

		if err := util.CheckRegionEpoch(&raftCmd, d.Region(), true); err != nil {
			return ErrResp(err)
		}

		if raftCmd.AdminRequest != nil {
			return d.applyAdminRaftCommand(entry, &raftCmd, kvWB)
		}
		return d.applyNormalRaftCommand(entry, &raftCmd, kvWB)
	}

	if entry.EntryType == pb.EntryType_EntryConfChange {
		var confChange pb.ConfChange
		err := proto.Unmarshal(entry.Data, &confChange)
		if err != nil {
			log.Errorf("%d failed to apply raft config change command, index: %d, term: %d, err:%+v", d.PeerId(), entry.Index, entry.Term, err)
			return ErrResp(err)
		}
		// apply admin request.
		return d.applyConfChangeRaftCommand(entry, confChange, kvWB)
	}

	log.Panic("unknown entry type: %+v", entry.EntryType)
	return nil
}

func (d *peerMsgHandler) proposalStr() string {
	res := ""
	for _, prop := range d.proposals {
		res = res + "(" + strconv.FormatUint(prop.index, 10) + ":" + strconv.FormatUint(prop.term, 10) + ")" + ","
	}
	return res
}

func (d *peerMsgHandler) applyRaftCmdToStateMachine(committedEnts []pb.Entry) error {

	for _, entry := range committedEnts {
		//log.Infof("%s, apply index:%d, commit command index: %d, term: %d", d.Tag, d.peerStorage.applyState.AppliedIndex, entry.Index, entry.Term)
		if entry.Index == d.peerStorage.applyState.AppliedIndex+1 {
			kvWB := new(engine_util.WriteBatch)
			d.peerStorage.applyState.AppliedIndex = entry.Index
			err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if err != nil {
				log.Errorf("set meta failed: %+v", err)
			}

			resp := d.applyRaftCommand(entry, kvWB)
			prop, notFound := d.findProposal(entry, true)
			if notFound {
				//if d.IsLeader() {
				//	log.Errorf("%d failed to find call back for entry %d, term: %d", d.PeerId(), entry.Index, entry.Term)
				//}
			} else {
				//log.Infof("%s send callback %s, resp: %+v", d.Tag, describeProposal(prop), resp)
				prop.cb.Done(resp)
			}
			if d.stopped {
				return nil
			}
			kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
			//log.Errorf("%d: commited entries: %+v", d.PeerId(), committedEnts)
			//log.Infof("%d applied index: %d", d.PeerId(), d.peerStorage.applyState.AppliedIndex)
		} else {
			log.Errorf("commited entries: %+v", committedEnts)
			log.Panicf("%s apply index now %d , entry(index: %d, term %d) applied failed ",
				d.Tag, d.peerStorage.applyState.AppliedIndex, entry.Index, entry.Term)
		}
	}
	return nil
}

func (d *peerMsgHandler) printProposals() {

	message := ""
	for _, p := range d.peer.proposals {
		message += describeProposal(p)
	}
	log.Errorf("proposals: %s", message)
}

func describeProposal(p *proposal) string {
	return "(" + strconv.Itoa(int(p.index)) + "," + strconv.Itoa(int(p.term)) + ")"
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).

	// 1.obtain ready rd.
	rd := d.RaftGroup.Ready()

	// 2. persist entries, call SaveReadyState
	state, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		log.Panicf("faild to save ready state %+v, err: %+v", rd, err)
	}
	if state != nil {
		clone := cloneRegion(state.Region)
		d.ctx.storeMeta.RWMutex.Lock()
		d.ctx.storeMeta.regions[d.regionId] = clone
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: clone})
		d.ctx.storeMeta.RWMutex.Unlock()
		d.LastCompactedIdx = d.peerStorage.truncatedIndex()
	}

	// 3. send message to peers.
	for _, msg := range rd.Messages {
		_ = d.sendRaftMessage(msg)
	}

	// if apply snapshot happens, not apply raft command to state machine.
	if state == nil {
		// 4. apply committed entries exec write cmd and get cmd.
		err = d.applyRaftCmdToStateMachine(rd.CommittedEntries)
		if err != nil {
			log.Panicf("failed to apply entries %+v, err:%+v", rd.CommittedEntries, err)
		}
	}

	// 5. modify in memory data, advance.
	d.RaftGroup.Advance(rd)

	if rd.SoftState != nil && d.IsLeader() {
		d.onSchedulerHeartbeatTick()
	}

}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func msgIsAdminRequest(msg *raft_cmdpb.RaftCmdRequest) bool {
	return msg.AdminRequest != nil
}

func msgIsSplitRegionRequest(msg *raft_cmdpb.RaftCmdRequest) bool {
	return msg.AdminRequest.Split != nil
}

func msgIsChangePeerRequest(msg *raft_cmdpb.RaftCmdRequest) bool {
	return msg.AdminRequest.ChangePeer != nil
}

func msgIsTransferLeaderRequest(msg *raft_cmdpb.RaftCmdRequest) bool {
	return msg.AdminRequest.TransferLeader != nil
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).

	// 2. convert data to bytes.
	data, err := proto.Marshal(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		log.Errorf("failed to marshal raft command: %+v", err)
		return
	}

	lastIndex := d.nextProposalIndex()
	lastTerm := d.Term()
	if msgIsAdminRequest(msg) && msgIsTransferLeaderRequest(msg) {

		d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.GetId())
		if d.PeerId() == msg.AdminRequest.TransferLeader.Peer.GetId() {
			cb.Done(&raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
					TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
				},
			})
		}

		return
	}
	// propose command.
	if msgIsAdminRequest(msg) && msgIsChangePeerRequest(msg) {
		if !d.msgHasSameRegion(msg) {
			log.Errorf("%s failed to propose conf change raft command, region not same, cur region: %+v, msg region: %+v", d.Tag, d.Region(), msg.GetHeader())
			return
		}
		//log.Errorf("confchange propose command detail: %+v", msg)
		context, err := proto.Marshal(msg)
		if err != nil {
			log.Errorf("failed to propose conf change raft command: %+v,", err)
			return
		}
		confChange := pb.ConfChange{
			ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
			NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
			Context:    context,
		}
		if confChange.ChangeType == pb.ConfChangeType_RemoveNode {
			if confChange.NodeId == d.PeerId() {
				// sending transfer leader to other nodes
				progressList := d.RaftGroup.GetProgress()
				if len(progressList) > 1 {
					for id, prs := range progressList {
						if prs.RecentActive && id != d.PeerId() {
							d.RaftGroup.TransferLeader(id)
							log.Errorf("%s failed to propose remove itself command, transfer leader to %d, progress: %+v", d.Tag, id, progressList)
							// TODO: cb.Done(error not leader?)
							return
						}
					}
					for id := range progressList {
						if id != d.PeerId() {
							d.RaftGroup.TransferLeader(id)
							log.Errorf("%s failed to propose remove itself command, transfer leader to %d, progress: %+v", d.Tag, id, progressList)
							return
						}
					}
					log.Errorf("%s failed to propose remove itself command, it find no recently active node. %+v", d.Tag, progressList)
					return
				}
			}
		}
		if err = d.RaftGroup.ProposeConfChange(confChange); err != nil {
			log.Errorf("failed to propose conf change raft cammand: %+v", err)
			return
		}
		// TODO: mannually apply config change command.
		// how to abort a failed config change ?
		// according to chapter 4 of phd thesis of Diego
		// there seems need a learner role.
		// _ = d.applyConfChangeRaftCommand(pb.Entry{}, confChange, new(engine_util.WriteBatch))

		log.Infof("%s propose a confChange command at %d", d.Tag, lastIndex)
	} else if msgIsAdminRequest(msg) && msgIsSplitRegionRequest(msg) {
		//log.Errorf("%s receive split region propose: %+v", d.Tag, msg)
		if err = d.RaftGroup.Propose(data); err != nil {
			log.Errorf("%d failed to propose raft command: %+v", d.PeerId(), err)
			return
		}
	} else {
		if err = d.RaftGroup.Propose(data); err != nil {
			log.Errorf("%d failed to propose raft command: %+v", d.PeerId(), err)
			return
		}
		//log.Infof("%s propose a normal command at %d", d.Tag, lastIndex)
	}

	// 1.storage callback to pendingCmd(proposals).

	d.peer.proposals = append(d.peer.proposals, &proposal{
		index: lastIndex,
		term:  lastTerm,
		cb:    cb,
	})

}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d, detail:%+v",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId(), msg)
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()
	//log.Infof("%s receive message from %+v, details:%+v", d.Tag, msg.FromPeer, msg)
	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func (d *peerMsgHandler) msgHasSameRegion(msg *raft_cmdpb.RaftCmdRequest) bool {
	left := d.Region()
	right := msg.GetHeader()
	if left.Id != right.RegionId {
		return false
	}
	if left.RegionEpoch.ConfVer != right.RegionEpoch.ConfVer {
		return false
	}
	if left.RegionEpoch.Version != right.RegionEpoch.Version {
		return false
	}
	return true
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
