package updater

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	watchdogObservationThreshold = 3
	watchdogMaxAttempts          = 3
	watchdogAttemptWindow        = 10 * time.Minute
	watchdogCooldown             = 15 * time.Minute
	watchdogReconcileInterval    = 30 * time.Second
	watchdogEventRetryInterval   = 5 * time.Second
)

type watchdogRuntime interface {
	ObserveWatchdog(context.Context) error
	RepairWatchdog(context.Context) error
	StreamWatchdogEvents(context.Context, func()) error
}

type Watchdog struct {
	store              *StateStore
	runtime            watchdogRuntime
	now                func() time.Time
	reconcileInterval  time.Duration
	eventRetryInterval time.Duration
	logf               func(string, ...any)
	hostGate           *HostOperationGate
}

func NewWatchdog(store *StateStore, runtime watchdogRuntime, gates ...*HostOperationGate) *Watchdog {
	hostGate := NewHostOperationGate()
	if len(gates) > 0 && gates[0] != nil {
		hostGate = gates[0]
	}
	return &Watchdog{
		store:              store,
		runtime:            runtime,
		now:                time.Now,
		reconcileInterval:  watchdogReconcileInterval,
		eventRetryInterval: watchdogEventRetryInterval,
		logf:               func(string, ...any) {},
		hostGate:           hostGate,
	}
}

func (watchdog *Watchdog) SetLogger(logf func(string, ...any)) {
	if watchdog != nil && logf != nil {
		watchdog.logf = logf
	}
}

func (watchdog *Watchdog) Run(ctx context.Context) {
	if watchdog == nil || watchdog.store == nil || watchdog.runtime == nil {
		return
	}
	triggers := make(chan struct{}, 1)
	notify := func() {
		select {
		case triggers <- struct{}{}:
		default:
		}
	}
	go watchdog.streamEvents(ctx, notify)
	ticker := time.NewTicker(watchdog.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-triggers:
		}
		if err := watchdog.Reconcile(ctx); err != nil {
			watchdog.logf("watchdog reconciliation failed")
		}
	}
}

func (watchdog *Watchdog) streamEvents(ctx context.Context, notify func()) {
	for ctx.Err() == nil {
		err := watchdog.runtime.StreamWatchdogEvents(ctx, notify)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			notify()
		}
		timer := time.NewTimer(watchdog.eventRetryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func (watchdog *Watchdog) Reconcile(ctx context.Context) error {
	if watchdog == nil || watchdog.store == nil || watchdog.runtime == nil {
		return fmt.Errorf("watchdog is not configured")
	}
	state, err := watchdog.store.Load(ctx)
	if err != nil {
		return fmt.Errorf("load watchdog state: %w", err)
	}
	now := watchdog.now().UTC()
	if state.DesiredState != DesiredRunning {
		return watchdog.store.Update(ctx, func(state *RuntimeState) error {
			suppressWatchdog(state)
			return nil
		})
	}

	observationErr := watchdog.runtime.ObserveWatchdog(ctx)
	if observationErr == nil {
		return watchdog.store.Update(ctx, func(state *RuntimeState) error {
			state.Watchdog.Status = WatchdogHealthy
			state.Watchdog.ConsecutiveFailures = 0
			state.Watchdog.CooldownUntil = time.Time{}
			state.Watchdog.LastObservedAt = now
			state.Watchdog.ErrorCode = ""
			state.Watchdog.Attempts = recentWatchdogAttempts(state.Watchdog.Attempts, now)
			return nil
		})
	}

	shouldRepair := false
	suppressed := false
	err = watchdog.store.Update(ctx, func(state *RuntimeState) error {
		if state.DesiredState != DesiredRunning {
			suppressWatchdog(state)
			suppressed = true
			return nil
		}
		state.Watchdog.LastObservedAt = now
		state.Watchdog.ErrorCode = "observation_failed"
		state.Watchdog.Attempts = recentWatchdogAttempts(state.Watchdog.Attempts, now)
		if state.Watchdog.CooldownUntil.After(now) {
			state.Watchdog.Status = WatchdogDegraded
			state.Watchdog.ConsecutiveFailures = 0
			return nil
		}
		state.Watchdog.CooldownUntil = time.Time{}
		state.Watchdog.ConsecutiveFailures++
		if state.Watchdog.ConsecutiveFailures < watchdogObservationThreshold {
			state.Watchdog.Status = WatchdogObserving
			return nil
		}
		state.Watchdog.ConsecutiveFailures = 0
		if len(state.Watchdog.Attempts) >= watchdogMaxAttempts {
			state.Watchdog.Status = WatchdogDegraded
			state.Watchdog.CooldownUntil = now.Add(watchdogCooldown)
			return nil
		}
		state.Watchdog.Attempts = append(state.Watchdog.Attempts, now)
		state.Watchdog.LastRepairAt = now
		state.Watchdog.Status = WatchdogRepairing
		shouldRepair = true
		return nil
	})
	if err != nil {
		return errors.Join(fmt.Errorf("watchdog observation failed"), err)
	}
	if !shouldRepair {
		if suppressed {
			return nil
		}
		return fmt.Errorf("watchdog observation failed")
	}
	state, err = watchdog.store.Load(ctx)
	if err != nil {
		return errors.Join(fmt.Errorf("watchdog observation failed"), err)
	}
	if state.DesiredState != DesiredRunning {
		persistErr := watchdog.store.Update(ctx, func(state *RuntimeState) error {
			suppressWatchdog(state)
			return nil
		})
		return persistErr
	}

	repairCtx, releaseRepair := watchdog.hostGate.BeginRepair(ctx)
	defer releaseRepair()
	state, err = watchdog.store.Load(repairCtx)
	if err != nil {
		return errors.Join(fmt.Errorf("watchdog observation failed"), err)
	}
	if state.DesiredState != DesiredRunning {
		return watchdog.store.Update(repairCtx, func(state *RuntimeState) error {
			suppressWatchdog(state)
			return nil
		})
	}
	repairErr := watchdog.runtime.RepairWatchdog(repairCtx)
	if repairCtx.Err() != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return watchdog.restoreInterruptedRepair(ctx)
	}
	if repairErr == nil {
		return watchdog.store.Update(ctx, func(state *RuntimeState) error {
			state.Watchdog.Status = WatchdogHealthy
			state.Watchdog.ConsecutiveFailures = 0
			state.Watchdog.CooldownUntil = time.Time{}
			state.Watchdog.LastObservedAt = now
			state.Watchdog.ErrorCode = ""
			return nil
		})
	}
	persistErr := watchdog.store.Update(ctx, func(state *RuntimeState) error {
		state.Watchdog.ConsecutiveFailures = 0
		state.Watchdog.ErrorCode = "repair_failed"
		if len(state.Watchdog.Attempts) >= watchdogMaxAttempts {
			state.Watchdog.Status = WatchdogDegraded
			state.Watchdog.CooldownUntil = now.Add(watchdogCooldown)
		} else {
			state.Watchdog.Status = WatchdogObserving
		}
		return nil
	})
	return errors.Join(fmt.Errorf("watchdog repair failed"), persistErr)
}

func (watchdog *Watchdog) restoreInterruptedRepair(ctx context.Context) error {
	return watchdog.store.Update(ctx, func(state *RuntimeState) error {
		if state.DesiredState != DesiredRunning || hasActiveJob(*state) || state.Watchdog.Status != WatchdogRepairing {
			return nil
		}
		attempts := state.Watchdog.Attempts
		if len(attempts) > 0 && attempts[len(attempts)-1].Equal(state.Watchdog.LastRepairAt) {
			state.Watchdog.Attempts = attempts[:len(attempts)-1]
		}
		state.Watchdog.Status = WatchdogObserving
		state.Watchdog.ConsecutiveFailures = 0
		state.Watchdog.LastRepairAt = time.Time{}
		state.Watchdog.ErrorCode = "observation_failed"
		return nil
	})
}

func suppressWatchdog(state *RuntimeState) {
	state.Watchdog.Status = WatchdogSuppressed
	state.Watchdog.ConsecutiveFailures = 0
	state.Watchdog.CooldownUntil = time.Time{}
	state.Watchdog.ErrorCode = ""
}

func recentWatchdogAttempts(attempts []time.Time, now time.Time) []time.Time {
	cutoff := now.Add(-watchdogAttemptWindow)
	recent := make([]time.Time, 0, len(attempts))
	for _, attempt := range attempts {
		if attempt.After(cutoff) {
			recent = append(recent, attempt.UTC())
		}
	}
	return recent
}
