package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	lock, err := txn.GetLock(req.GetKey())
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts < req.GetVersion() {
		return &kvrpcpb.GetResponse{
			RegionError: nil,
			Error: &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					Key:         req.GetKey(),
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					LockTtl:     lock.Ttl,
				},
			},
		}, nil
	}
	value, err := txn.GetValue(req.GetKey())
	if err != nil {
		return nil, err
	}
	if len(value) > 0 {
		return &kvrpcpb.GetResponse{
			RegionError: nil,
			Error:       nil,
			Value:       value,
		}, nil
	} else {
		return &kvrpcpb.GetResponse{
			RegionError: nil,
			Error:       nil,
			Value:       nil,
			NotFound:    true,
		}, nil
	}
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	// Abort on write after our start timestamp...
	var keyErrors []*kvrpcpb.KeyError
	for _, mutation := range req.GetMutations() {
		write, ts, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && txn.StartTS <= ts {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    txn.StartTS,
					ConflictTs: ts,
					Key:        mutation.GetKey(),
					Primary:    req.GetPrimaryLock(),
				},
			})
		}
	}

	// ... or lock at any timestamp
	for _, mutation := range req.GetMutations() {
		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts <= txn.StartTS {
			keyErrors = append(keyErrors,
				&kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						Key:         mutation.GetKey(),
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						LockTtl:     lock.Ttl,
					},
				},
			)
		}
	}
	if len(keyErrors) > 0 {
		return &kvrpcpb.PrewriteResponse{
			Errors: keyErrors,
		}, nil
	}

	for _, mutation := range req.GetMutations() {
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mutation.GetKey(), mutation.GetValue())
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mutation.GetKey())
		}
		lock := mvcc.Lock{
			Primary: req.GetPrimaryLock(),
			Ts:      req.GetStartVersion(),
			Ttl:     req.GetLockTtl(),
		}
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			lock.Kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			lock.Kind = mvcc.WriteKindDelete
		}
		txn.PutLock(mutation.GetKey(), &lock)
	}
	writes := txn.Writes()
	err = server.storage.Write(req.GetContext(), writes)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.PrewriteResponse{}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	var keyErrors []*kvrpcpb.KeyError
	for _, key := range req.GetKeys() {

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			_, ts, err := txn.CurrentWrite(key)
			if err != nil {
				return nil, err
			}
			// txn has been committed.
			if ts == req.GetCommitVersion() {
				return &kvrpcpb.CommitResponse{}, nil
			}
			_, ts, err = txn.MostRecentWrite(key)
			if err != nil {
				return nil, err
			}
			if ts > req.GetCommitVersion() {
				return &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{Abort: "txn abort"},
				}, nil
			} else if ts == req.GetCommitVersion() {
				return &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{Retryable: "txn should retry"},
				}, nil
			} else {
				// missing pre write.
				return &kvrpcpb.CommitResponse{}, nil
			}
		} else if lock.Ts != req.GetStartVersion() {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Retryable: "should retry",
			})
		}
	}

	if len(keyErrors) > 0 {
		return &kvrpcpb.CommitResponse{
			Error: keyErrors[0],
		}, nil
	}
	for _, key := range req.GetKeys() {
		lock, err := txn.GetLock(key)
		if err != nil {
			log.Panicf("failed to get lock, err: %+v", err)
		}
		txn.DeleteLock(key)
		txn.PutWrite(key, req.GetCommitVersion(), &mvcc.Write{
			StartTS: lock.Ts,
			Kind:    lock.Kind,
		})
	}

	writes := txn.Writes()
	err = server.storage.Write(req.GetContext(), writes)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return &kvrpcpb.ScanResponse{}, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetLockTs())
	write, ts, err := txn.CurrentWrite(req.GetPrimaryKey())
	if err != nil {
		return nil, err
	}
	if write != nil {
		if write.Kind == mvcc.WriteKindRollback {
			return &kvrpcpb.CheckTxnStatusResponse{}, nil
		} else {
			return &kvrpcpb.CheckTxnStatusResponse{CommitVersion: ts}, nil
		}
	}

	lock, err := txn.GetLock(req.GetPrimaryKey())
	if err != nil {
		return nil, err
	}
	// lock not exists
	if lock == nil {
		txn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &mvcc.Write{
			Kind:    mvcc.WriteKindRollback,
			StartTS: req.GetLockTs(),
		})
		writes := txn.Writes()
		err := server.storage.Write(req.GetContext(), writes)
		if err != nil {
			return nil, err
		}
		return &kvrpcpb.CheckTxnStatusResponse{
			Action: kvrpcpb.Action_LockNotExistRollback,
		}, nil
	}

	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.GetCurrentTs()) {
		// timeout, abort txn.
		txn.DeleteLock(req.GetPrimaryKey())
		txn.DeleteValue(req.GetPrimaryKey())
		txn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &mvcc.Write{StartTS: lock.Ts, Kind: mvcc.WriteKindRollback})
		writes := txn.Writes()
		err := server.storage.Write(req.GetContext(), writes)
		if err != nil {
			return nil, err
		}
		return &kvrpcpb.CheckTxnStatusResponse{
			Action: kvrpcpb.Action_TTLExpireRollback,
		}, nil

	} else {
		// return status.
		return &kvrpcpb.CheckTxnStatusResponse{
			LockTtl: lock.Ttl,
			Action:  kvrpcpb.Action_NoAction,
		}, nil
	}
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	// check locks and
	var keyError *kvrpcpb.KeyError
	for _, key := range req.GetKeys() {
		if keyError != nil {
			break
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			write, ts, err := txn.CurrentWrite(key)
			if err != nil {
				return nil, err
			}
			// txn has been committed.
			if write != nil && write.StartTS == req.GetStartVersion() {
				return &kvrpcpb.BatchRollbackResponse{Error: &kvrpcpb.KeyError{Abort: "rollback abort."}}, nil
			}
			_, ts, err = txn.MostRecentWrite(key)
			if err != nil {
				return nil, err
			}
			if ts > req.GetStartVersion() {
				// has been roll backed or committed.
				return &kvrpcpb.BatchRollbackResponse{}, nil
			} else if ts == req.GetStartVersion() {
				// do nothing, duplicate request.
				return &kvrpcpb.BatchRollbackResponse{}, nil
			} else {
				// has been roll backed.
				// missing pre write or
				txn.PutWrite(key, req.GetStartVersion(), &mvcc.Write{
					StartTS: req.GetStartVersion(),
					Kind:    mvcc.WriteKindRollback,
				})
			}
		} else if lock.Ts != req.GetStartVersion() {
			// lock by another transaction.
			txn.PutWrite(key, req.GetStartVersion(), &mvcc.Write{
				StartTS: req.GetStartVersion(),
				Kind:    mvcc.WriteKindRollback,
			})
		} else if lock.Ts == req.GetStartVersion() {
			// prewrite not write.
			txn.DeleteLock(key)
			txn.DeleteValue(key)
			txn.PutWrite(key, req.GetStartVersion(), &mvcc.Write{
				StartTS: req.GetStartVersion(),
				Kind:    mvcc.WriteKindRollback,
			})
		}
	}

	writes := txn.Writes()
	err = server.storage.Write(req.GetContext(), writes)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.BatchRollbackResponse{}, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	iterator := txn.Reader.IterCF(engine_util.CfLock)
	for ; iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		//keyWithTs := item.Key()
		//userKey := mvcc.DecodeUserKey(keyWithTs)
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return nil, err
		}
		if lock.Ts == req.GetStartVersion() {
			txn.DeleteLock(item.Key())
			if req.GetCommitVersion() == 0 {
				txn.DeleteValue(item.Key())
				txn.PutWrite(item.Key(), lock.Ts, &mvcc.Write{
					StartTS: lock.Ts,
					Kind:    mvcc.WriteKindRollback,
				})
			} else {
				txn.PutWrite(item.Key(), req.GetCommitVersion(), &mvcc.Write{
					StartTS: lock.Ts,
					Kind:    lock.Kind,
				})
			}
		}
	}

	writes := txn.Writes()
	err = server.storage.Write(req.GetContext(), writes)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.ResolveLockResponse{}, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
