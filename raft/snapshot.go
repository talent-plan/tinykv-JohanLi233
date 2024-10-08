package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) sendSnapshot(to uint64) {
	newSnapshot, err := r.RaftLog.storage.Snapshot()
	if err == ErrSnapshotTemporarilyUnavailable {
		return
	}

	request := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &newSnapshot,
	}
	r.msgs = append(r.msgs, request)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	response := &pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.GetFrom(),
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	}

	if m.GetTerm() < r.Term {
		response.Reject = true
		r.msgs = append(r.msgs, *response)
		return
	}

	meta := m.GetSnapshot().GetMetadata()

	if meta.GetIndex() < r.RaftLog.committed { // CompactLogRequest will be applied soon
		response.Reject = true
		response.Index = r.RaftLog.committed
		r.msgs = append(r.msgs, *response)
		return
	}

	// 尽可能无条件接收，并直接覆盖所有状态，ready 收到非空 snapshot 后会直接应用到 DB 中
	r.becomeFollower(m.GetTerm(), m.GetFrom())
	response.Term = m.GetTerm()
	response.Index = meta.GetIndex()

	{ // 覆盖 RaftLog 所有状态
		r.RaftLog.applied = meta.GetIndex()
		r.RaftLog.committed = meta.GetIndex()
		r.RaftLog.stabled = meta.GetIndex()
		r.RaftLog.entries = []pb.Entry{{Term: meta.GetTerm(), Index: meta.GetIndex()}}
		// 下次 handleReady 的 saveReadyState 会调用 peer storage 的 apply snapshot
		r.RaftLog.pendingSnapshot = m.GetSnapshot()
	}

	r.Prs = make(map[uint64]*Progress, len(meta.GetConfState().GetNodes()))
	r.votes = make(map[uint64]bool, len(meta.GetConfState().GetNodes()))
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{
			Match: r.RaftLog.TruncatedIndex(),
			Next:  r.RaftLog.LastIndex() + 1,
		}
		r.votes[peer] = false
	}
	r.msgs = append(r.msgs, *response)
}
