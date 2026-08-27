package agents

// The server side of a machine shell: OpenPty picks the agent's live
// session and returns the event pipe the management stream drains;
// input, resize and close address the pty by its server-minted id.
// A pipe dies with its pty, its stream, or its agent — whichever goes
// first; there is nothing to reconnect.

import (
	"fmt"
	"sync"

	"github.com/google/uuid"

	agentpb "github.com/graphene-ci/agent/pkg/proto/agent/v1"
	"github.com/graphene-ci/pipeline/pkg/id"
)

// PtyEvent is one thing the shell did: output bytes, or its death.
type PtyEvent struct {
	Data   []byte
	Closed bool
	Exit   int32
	Msg    string
}

// ptyPipe carries one shell's events to whoever opened it.
type ptyPipe struct {
	namespace string
	agentId   id.AgentId
	sess      *session
	events    chan PtyEvent

	closeOnce sync.Once
}

// OpenPty starts a shell on the machine and returns the pty id, the
// event pipe, and the namespace-checked close. The caller (the
// management stream) owns the session: when it returns, it closes.
func (r *Registry) OpenPty(namespace string, agentId id.AgentId, cols, rows uint32) (string, <-chan PtyEvent, func(), error) {
	r.mu.Lock()
	s, ok := r.agents[agentKey{namespace: namespace, agentId: agentId}]
	r.mu.Unlock()
	if !ok {
		return "", nil, nil, fmt.Errorf("agent of machine %q is not connected", agentId)
	}
	ptyId := uuid.NewString()
	pipe := &ptyPipe{
		namespace: namespace,
		agentId:   agentId,
		sess:      s,
		// Buffered: the agent keeps frames small; a slow reader gets
		// backpressure through the drain loop, not a dropped shell.
		events: make(chan PtyEvent, 64),
	}
	r.ptyMu.Lock()
	if r.ptys == nil {
		r.ptys = map[string]*ptyPipe{}
	}
	r.ptys[ptyId] = pipe
	r.ptyMu.Unlock()
	if err := s.send(&agentpb.SessionResponse{Body: &agentpb.SessionResponse_OpenPty{
		OpenPty: &agentpb.OpenPty{PtyId: ptyId, Cols: cols, Rows: rows},
	}}); err != nil {
		r.dropPty(ptyId)
		return "", nil, nil, err
	}
	closeFn := func() {
		_ = s.send(&agentpb.SessionResponse{Body: &agentpb.SessionResponse_ClosePty{
			ClosePty: &agentpb.ClosePty{PtyId: ptyId},
		}})
		r.dropPty(ptyId)
	}
	return ptyId, pipe.events, closeFn, nil
}

// PtyInput writes keystrokes into the shell. The namespace must match
// the pipe's — a session id is not a capability across namespaces.
func (r *Registry) PtyInput(namespace, ptyId string, data []byte) error {
	pipe, err := r.pty(namespace, ptyId)
	if err != nil {
		return err
	}
	return pipe.sess.send(&agentpb.SessionResponse{Body: &agentpb.SessionResponse_PtyInput{
		PtyInput: &agentpb.PtyInput{PtyId: ptyId, Data: data},
	}})
}

// PtyResize follows the viewer's terminal geometry.
func (r *Registry) PtyResize(namespace, ptyId string, cols, rows uint32) error {
	pipe, err := r.pty(namespace, ptyId)
	if err != nil {
		return err
	}
	return pipe.sess.send(&agentpb.SessionResponse{Body: &agentpb.SessionResponse_PtyResize{
		PtyResize: &agentpb.PtyResize{PtyId: ptyId, Cols: cols, Rows: rows},
	}})
}

// PtyClose buries the shell on the viewer's explicit ask.
func (r *Registry) PtyClose(namespace, ptyId string) error {
	pipe, err := r.pty(namespace, ptyId)
	if err != nil {
		return err
	}
	err = pipe.sess.send(&agentpb.SessionResponse{Body: &agentpb.SessionResponse_ClosePty{
		ClosePty: &agentpb.ClosePty{PtyId: ptyId},
	}})
	r.dropPty(ptyId)
	return err
}

func (r *Registry) pty(namespace, ptyId string) (*ptyPipe, error) {
	r.ptyMu.Lock()
	pipe := r.ptys[ptyId]
	r.ptyMu.Unlock()
	if pipe == nil || pipe.namespace != namespace {
		return nil, fmt.Errorf("no such pty session")
	}
	return pipe, nil
}

// dropPty forgets the pipe and wakes its drain loop.
func (r *Registry) dropPty(ptyId string) {
	r.ptyMu.Lock()
	pipe := r.ptys[ptyId]
	delete(r.ptys, ptyId)
	r.ptyMu.Unlock()
	if pipe != nil {
		pipe.closeOnce.Do(func() { close(pipe.events) })
	}
}

// routePtyOutput lands an agent's output frame on its pipe. A full
// pipe drops the frame rather than stalling the WHOLE agent session —
// the shared stream carries container commands too.
func (r *Registry) routePtyOutput(out *agentpb.PtyOutput) {
	r.ptyMu.Lock()
	pipe := r.ptys[out.GetPtyId()]
	r.ptyMu.Unlock()
	if pipe == nil {
		return
	}
	select {
	case pipe.events <- PtyEvent{Data: out.GetData()}:
	default:
		r.log.Warn("pty output dropped: slow reader")
	}
}

// routePtyClosed reports the shell's death and buries the pipe.
func (r *Registry) routePtyClosed(cl *agentpb.PtyClosed) {
	r.ptyMu.Lock()
	pipe := r.ptys[cl.GetPtyId()]
	r.ptyMu.Unlock()
	if pipe == nil {
		return
	}
	select {
	case pipe.events <- PtyEvent{Closed: true, Exit: cl.GetExitCode(), Msg: cl.GetMessage()}:
	default:
	}
	r.dropPty(cl.GetPtyId())
}

// dropSessionPtys buries every pipe of a disconnected agent.
func (r *Registry) dropSessionPtys(s *session) {
	r.ptyMu.Lock()
	ids := make([]string, 0)
	for ptyId, pipe := range r.ptys {
		if pipe.sess == s {
			ids = append(ids, ptyId)
		}
	}
	r.ptyMu.Unlock()
	for _, ptyId := range ids {
		r.dropPty(ptyId)
	}
}
