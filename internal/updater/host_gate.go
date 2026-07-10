package updater

import (
	"context"
	"sync"
)

// HostOperationGate serializes watchdog repair with control-plane mutations
// that enter upgrading, maintenance, or deprovisioned state. A mutation first
// cancels the active repair so bounded host commands stop before the desired
// state is committed and the caller intentionally changes the topology.
type HostOperationGate struct {
	operation sync.Mutex
	activeMu  sync.Mutex
	active    *hostRepairSession
}

type hostRepairSession struct {
	cancel context.CancelFunc
}

func NewHostOperationGate() *HostOperationGate {
	return &HostOperationGate{}
}

func (gate *HostOperationGate) BeginRepair(parent context.Context) (context.Context, func()) {
	gate.operation.Lock()
	ctx, cancel := context.WithCancel(parent)
	session := &hostRepairSession{cancel: cancel}
	gate.activeMu.Lock()
	gate.active = session
	gate.activeMu.Unlock()
	return ctx, func() {
		cancel()
		gate.activeMu.Lock()
		if gate.active == session {
			gate.active = nil
		}
		gate.activeMu.Unlock()
		gate.operation.Unlock()
	}
}

func (gate *HostOperationGate) BeginMutation() func() {
	gate.activeMu.Lock()
	if gate.active != nil {
		gate.active.cancel()
	}
	gate.activeMu.Unlock()
	gate.operation.Lock()
	return gate.operation.Unlock
}
