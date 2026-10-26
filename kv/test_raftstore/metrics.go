package test_raftstore

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

type testMetrics struct {
	requests  uint64
	responses uint64
	errors    uint64
	timeouts  uint64
	puts      uint64
	gets      uint64
	scans     uint64
	deletes   uint64
}

func (m *testMetrics) observeRequest(reqs []*raft_cmdpb.Request) {
	atomic.AddUint64(&m.requests, 1)
	if len(reqs) == 0 {
		return
	}
	switch reqs[0].GetCmdType() {
	case raft_cmdpb.CmdType_Put:
		atomic.AddUint64(&m.puts, 1)
	case raft_cmdpb.CmdType_Get:
		atomic.AddUint64(&m.gets, 1)
	case raft_cmdpb.CmdType_Snap:
		atomic.AddUint64(&m.scans, 1)
	case raft_cmdpb.CmdType_Delete:
		atomic.AddUint64(&m.deletes, 1)
	}
}

func (m *testMetrics) observeResponse() { atomic.AddUint64(&m.responses, 1) }
func (m *testMetrics) observeError()    { atomic.AddUint64(&m.errors, 1) }
func (m *testMetrics) observeTimeout()  { atomic.AddUint64(&m.timeouts, 1) }

func (m *testMetrics) report(elapsed time.Duration, regions int) string {
	requests := atomic.LoadUint64(&m.requests)
	responses := atomic.LoadUint64(&m.responses)
	qps := float64(requests) / elapsed.Seconds()
	responseQPS := float64(responses) / elapsed.Seconds()
	return fmt.Sprintf("test metrics elapsed=%s requests=%d responses=%d in_flight=%d qps=%.1f response_qps=%.1f errors=%d timeouts=%d puts=%d gets=%d scans=%d deletes=%d regions=%d",
		elapsed.Round(time.Second), requests, responses, requests-responses, qps, responseQPS,
		atomic.LoadUint64(&m.errors), atomic.LoadUint64(&m.timeouts),
		atomic.LoadUint64(&m.puts), atomic.LoadUint64(&m.gets), atomic.LoadUint64(&m.scans),
		atomic.LoadUint64(&m.deletes), regions)
}
