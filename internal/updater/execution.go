package updater

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const executionTotalSteps = 7
const maxRecoveryAttempts = 3

type JobStep string

const (
	JobStepValidate      JobStep = "validating"
	JobStepPulling       JobStep = "pulling"
	JobStepStopping      JobStep = "stopping"
	JobStepMigrating     JobStep = "migrating"
	JobStepStarting      JobStep = "starting"
	JobStepCheckTarget   JobStep = "health_check"
	JobStepRestoreBackup JobStep = "restore_backup"
	JobStepCheckRestored JobStep = "check_restored"
	JobStepRestart       JobStep = "restart"
	JobStepComplete      JobStep = "complete"
)

var upgradePhaseRanks = map[JobStatus]int{
	JobValidating:  0,
	JobBackingUp:   1,
	JobPulling:     2,
	JobStopping:    3,
	JobMigrating:   4,
	JobStarting:    5,
	JobHealthCheck: 6,
}

func (status JobStatus) valid() bool {
	if _, ok := upgradePhaseRanks[status]; ok {
		return true
	}
	switch status {
	case JobQueued, JobRollingBack, JobRestarting, JobSucceeded, JobFailed, JobRolledBack:
		return true
	default:
		return false
	}
}

func (status JobStatus) active() bool {
	_, upgrade := upgradePhaseRanks[status]
	return upgrade || status == JobQueued || status == JobRollingBack || status == JobRestarting
}

func (job Job) validateExecutionState() error {
	if job.TotalSteps < 0 || job.CompletedSteps < 0 || job.CompletedSteps > job.TotalSteps {
		return fmt.Errorf("progress is invalid")
	}
	if job.RecoveryAttempts < 0 || job.RecoveryAttempts > maxRecoveryAttempts {
		return fmt.Errorf("recovery attempts are invalid")
	}
	if job.Status.active() {
		if job.TotalSteps != executionTotalSteps {
			return fmt.Errorf("active job total_steps must be %d", executionTotalSteps)
		}
		if job.CurrentStep == "" {
			return fmt.Errorf("active job current_step is required")
		}
	}
	if job.Status == JobQueued && job.CurrentStep != JobStepValidate {
		return fmt.Errorf("queued job must start at validation")
	}
	if job.Status == JobRollingBack && job.RecoveryPoint == nil {
		return fmt.Errorf("rolling_back requires a recovery point")
	}
	if job.Status == JobRestarting && job.CurrentStep != JobStepRestart {
		return fmt.Errorf("restarting job must use the restart step")
	}
	if job.RecoveryPoint != nil {
		if err := validateBackupMetadataShape(*job.RecoveryPoint); err != nil {
			return fmt.Errorf("recovery point: %w", err)
		}
		if job.RecoveryPoint.JobID != job.ID || job.RecoveryPoint.Version != job.CurrentVersion {
			return fmt.Errorf("recovery point does not match the job source")
		}
	}
	if job.Status == JobSucceeded || job.Status == JobFailed || job.Status == JobRolledBack {
		if job.CurrentStep != JobStepComplete {
			return fmt.Errorf("terminal job must be complete")
		}
	}
	if job.Status == JobSucceeded && (!job.ServiceAvailable || job.ErrorCode != "" || job.ErrorMessage != "") {
		return fmt.Errorf("succeeded job has inconsistent availability or error")
	}
	if (job.Status == JobFailed || job.Status == JobRolledBack) && job.ErrorCode == "" {
		return fmt.Errorf("failed or rolled back job requires error_code")
	}
	return nil
}

type UpgradeRuntime interface {
	PrepareBackup(context.Context, Job, Plan, func(JobStatus) error) (BackupMetadata, error)
	ActivateTarget(context.Context, Manifest, func(JobStatus) error) error
	CheckTarget(context.Context, Manifest) error
	RestoreBackup(context.Context, BackupMetadata) error
	CheckRestored(context.Context, BackupMetadata) error
	RestartCurrent(context.Context, Job) error
}

type JobEngine struct {
	store   *StateStore
	runtime UpgradeRuntime
	now     func() time.Time
}

func NewJobEngine(store *StateStore, runtime UpgradeRuntime) *JobEngine {
	return &JobEngine{store: store, runtime: runtime, now: time.Now}
}

// RunActive advances the single persisted active job. Every external mutation
// has a durable checkpoint that makes replay after process restart idempotent.
func (engine *JobEngine) RunActive(ctx context.Context) error {
	if engine == nil || engine.store == nil || engine.runtime == nil {
		return fmt.Errorf("job engine is not configured")
	}
	job, plan, found, err := engine.activeJob(ctx)
	if err != nil || !found {
		return err
	}
	if job.Status == JobQueued {
		if err := engine.updateJob(ctx, job.ID, func(job *Job, _ *RuntimeState) {
			job.Status = JobValidating
			job.CurrentStep = JobStepValidate
		}); err != nil {
			return err
		}
		job.Status = JobValidating
		job.CurrentStep = JobStepValidate
	}
	if job.Status == JobRollingBack {
		return engine.resumeRollback(ctx, job)
	}
	if job.Status == JobRestarting {
		return engine.resumeRestart(ctx, job)
	}

	switch job.Status {
	case JobValidating, JobBackingUp:
		recovery, prepareErr := engine.runtime.PrepareBackup(ctx, job, plan, engine.progress(ctx, job.ID))
		if prepareErr != nil {
			if err := engine.finishBeforeMutationFailure(ctx, job.ID, "backup_failed", errorServiceAvailable(prepareErr)); err != nil {
				return errors.Join(prepareErr, err)
			}
			return fmt.Errorf("prepare recovery point: %w", prepareErr)
		}
		if err := validateBackupMetadataShape(recovery); err != nil {
			validationErr := fmt.Errorf("prepared recovery point is invalid: %w", err)
			if persistErr := engine.finishBeforeMutationFailure(ctx, job.ID, "backup_invalid", true); persistErr != nil {
				return errors.Join(validationErr, persistErr)
			}
			return validationErr
		}
		if recovery.JobID != job.ID || recovery.Version != job.CurrentVersion {
			validationErr := fmt.Errorf("prepared recovery point does not match job")
			if persistErr := engine.finishBeforeMutationFailure(ctx, job.ID, "backup_invalid", true); persistErr != nil {
				return errors.Join(validationErr, persistErr)
			}
			return validationErr
		}
		if err := engine.updateJob(ctx, job.ID, func(job *Job, state *RuntimeState) {
			for priorID, prior := range state.Jobs {
				if priorID != job.ID && !prior.Status.active() && prior.RecoveryPoint != nil {
					prior.RecoveryPoint = nil
					state.Jobs[priorID] = prior
				}
			}
			job.RecoveryPoint = &recovery
			job.Status = JobPulling
			job.CompletedSteps = 2
			job.ServiceAvailable = true
			job.CurrentStep = JobStepPulling
		}); err != nil {
			return err
		}
		job.RecoveryPoint = &recovery
		job.Status = JobPulling
		job.CompletedSteps = 2
		job.ServiceAvailable = true
		job.CurrentStep = JobStepPulling
		fallthrough
	case JobPulling, JobStopping, JobMigrating, JobStarting:
		if activateErr := engine.runtime.ActivateTarget(ctx, plan.Manifest, engine.progress(ctx, job.ID)); activateErr != nil {
			if !errorMutationStarted(activateErr) {
				persistErr := engine.finishBeforeMutationFailure(ctx, job.ID, "target_activation_failed", errorServiceAvailable(activateErr))
				return errors.Join(fmt.Errorf("activate target before host mutation: %w", activateErr), persistErr)
			}
			return engine.beginAndRunRollback(ctx, job, "target_activation_failed", activateErr)
		}
		if err := engine.updateJob(ctx, job.ID, func(job *Job, _ *RuntimeState) {
			job.Status = JobHealthCheck
			job.CompletedSteps = 6
			job.ServiceAvailable = false
			job.CurrentStep = JobStepCheckTarget
		}); err != nil {
			return err
		}
		job.Status = JobHealthCheck
		job.CompletedSteps = 6
		job.ServiceAvailable = false
		job.CurrentStep = JobStepCheckTarget
		fallthrough
	case JobHealthCheck:
		if healthErr := engine.runtime.CheckTarget(ctx, plan.Manifest); healthErr != nil {
			return engine.beginAndRunRollback(ctx, job, "target_health_failed", healthErr)
		}
		return engine.updateJob(ctx, job.ID, func(job *Job, state *RuntimeState) {
			job.Status = JobSucceeded
			job.CurrentStep = JobStepComplete
			job.CompletedSteps = executionTotalSteps
			job.ServiceAvailable = true
			job.LastSafeVersion = job.TargetVersion
			job.ErrorCode = ""
			job.ErrorMessage = ""
			state.DesiredState = DesiredRunning
		})
	default:
		return fmt.Errorf("active job %s has unsupported status %q", job.ID, job.Status)
	}
}

func (engine *JobEngine) progress(ctx context.Context, jobID string) func(JobStatus) error {
	return func(next JobStatus) error {
		nextRank, ok := upgradePhaseRank(next)
		if !ok {
			return fmt.Errorf("job phase %q is not an upgrade phase", next)
		}
		return engine.updateJob(ctx, jobID, func(job *Job, _ *RuntimeState) {
			currentRank, currentIsUpgrade := upgradePhaseRank(job.Status)
			if currentIsUpgrade && currentRank > nextRank {
				return
			}
			job.Status = next
			job.CurrentStep = JobStep(next)
			job.CompletedSteps = nextRank
			switch next {
			case JobBackingUp, JobStopping, JobMigrating, JobStarting:
				job.ServiceAvailable = false
			case JobPulling:
				job.ServiceAvailable = true
			}
		})
	}
}

func upgradePhaseRank(status JobStatus) (int, bool) {
	rank, ok := upgradePhaseRanks[status]
	return rank, ok
}

func (engine *JobEngine) beginAndRunRollback(ctx context.Context, job Job, code string, cause error) error {
	if job.RecoveryPoint == nil {
		persistErr := engine.finishBeforeMutationFailure(ctx, job.ID, code, false)
		return errors.Join(cause, persistErr)
	}
	if err := engine.updateJob(ctx, job.ID, func(job *Job, _ *RuntimeState) {
		job.Status = JobRollingBack
		job.CurrentStep = JobStepRestoreBackup
		job.ServiceAvailable = false
		job.ErrorCode = code
		job.ErrorMessage = "The target release could not be verified. Restoring the previous release."
	}); err != nil {
		return errors.Join(cause, err)
	}
	job.Status = JobRollingBack
	job.CurrentStep = JobStepRestoreBackup
	job.ServiceAvailable = false
	job.ErrorCode = code
	job.ErrorMessage = "The target release could not be verified. Restoring the previous release."
	rollbackErr := engine.resumeRollback(ctx, job)
	if rollbackErr != nil {
		return errors.Join(cause, rollbackErr)
	}
	return fmt.Errorf("%s: %w", code, cause)
}

func (engine *JobEngine) resumeRollback(ctx context.Context, job Job) error {
	if job.RecoveryPoint == nil {
		return fmt.Errorf("rollback recovery point is missing")
	}
	if job.CurrentStep == JobStepRestoreBackup {
		if err := engine.updateJob(ctx, job.ID, func(job *Job, _ *RuntimeState) {
			job.RecoveryAttempts++
		}); err != nil {
			return err
		}
		job.RecoveryAttempts++
		if err := engine.runtime.RestoreBackup(ctx, *job.RecoveryPoint); err != nil {
			return engine.recordRecoveryFailure(ctx, job, fmt.Errorf("restore recovery point: %w", err))
		}
		if err := engine.updateJob(ctx, job.ID, func(job *Job, _ *RuntimeState) {
			job.CurrentStep = JobStepCheckRestored
		}); err != nil {
			return err
		}
		job.CurrentStep = JobStepCheckRestored
	}
	if job.CurrentStep != JobStepCheckRestored {
		return fmt.Errorf("rollback job %s has unsupported step %q", job.ID, job.CurrentStep)
	}
	if err := engine.runtime.CheckRestored(ctx, *job.RecoveryPoint); err != nil {
		// A future retry repeats the full idempotent restore, not only the health
		// probe, so partially restored state cannot be accepted.
		return engine.recordRecoveryFailure(ctx, job, fmt.Errorf("verify restored release: %w", err))
	}
	return engine.updateJob(ctx, job.ID, func(job *Job, state *RuntimeState) {
		job.Status = JobRolledBack
		job.CurrentStep = JobStepComplete
		job.ServiceAvailable = true
		job.LastSafeVersion = job.RecoveryPoint.Version
		job.ErrorMessage = "The target release failed validation. The previous release was restored."
		state.DesiredState = DesiredRunning
	})
}

func (engine *JobEngine) recordRecoveryFailure(ctx context.Context, job Job, cause error) error {
	persistErr := engine.updateJob(ctx, job.ID, func(stored *Job, state *RuntimeState) {
		stored.ServiceAvailable = false
		if stored.RecoveryAttempts >= maxRecoveryAttempts {
			stored.Status = JobFailed
			stored.CurrentStep = JobStepComplete
			stored.ErrorCode = "rollback_failed"
			stored.ErrorMessage = "Automatic rollback could not restore service. Manual rollback or restart is available."
			state.DesiredState = DesiredMaintenance
			return
		}
		stored.CurrentStep = JobStepRestoreBackup
	})
	return errors.Join(cause, persistErr)
}

func (engine *JobEngine) resumeRestart(ctx context.Context, job Job) error {
	if err := engine.updateJob(ctx, job.ID, func(job *Job, _ *RuntimeState) {
		job.RecoveryAttempts++
	}); err != nil {
		return err
	}
	job.RecoveryAttempts++
	if err := engine.runtime.RestartCurrent(ctx, job); err != nil {
		cause := fmt.Errorf("restart current message-server: %w", err)
		persistErr := engine.updateJob(ctx, job.ID, func(stored *Job, state *RuntimeState) {
			stored.ServiceAvailable = false
			if stored.RecoveryAttempts >= maxRecoveryAttempts {
				stored.Status = JobFailed
				stored.CurrentStep = JobStepComplete
				stored.ErrorCode = "restart_failed"
				stored.ErrorMessage = "The service could not be restarted. Retry restart or use the committed rollback."
				state.DesiredState = DesiredMaintenance
			}
		})
		return errors.Join(cause, persistErr)
	}
	return engine.updateJob(ctx, job.ID, func(job *Job, state *RuntimeState) {
		job.Status = JobFailed
		job.CurrentStep = JobStepComplete
		job.ServiceAvailable = true
		if job.LastSafeVersion == "" {
			job.LastSafeVersion = job.CurrentVersion
		}
		state.DesiredState = DesiredRunning
	})
}

func (engine *JobEngine) finishBeforeMutationFailure(ctx context.Context, jobID, code string, serviceAvailable bool) error {
	return engine.updateJob(ctx, jobID, func(job *Job, state *RuntimeState) {
		job.Status = JobFailed
		job.CurrentStep = JobStepComplete
		job.ServiceAvailable = serviceAvailable
		job.LastSafeVersion = job.CurrentVersion
		job.ErrorCode = code
		job.ErrorMessage = "The release change was stopped before the running service was changed."
		state.DesiredState = DesiredRunning
	})
}

type availabilityError interface {
	ServiceAvailable() bool
}

func errorServiceAvailable(err error) bool {
	var availability availabilityError
	if errors.As(err, &availability) {
		return availability.ServiceAvailable()
	}
	return true
}

type mutationStartedError interface {
	MutationStarted() bool
}

func errorMutationStarted(err error) bool {
	var mutation mutationStartedError
	return errors.As(err, &mutation) && mutation.MutationStarted()
}

func (engine *JobEngine) activeJob(ctx context.Context) (Job, Plan, bool, error) {
	state, err := engine.store.Load(ctx)
	if err != nil {
		return Job{}, Plan{}, false, err
	}
	var active *Job
	for _, candidate := range state.Jobs {
		if !candidate.Status.active() {
			continue
		}
		if active != nil {
			return Job{}, Plan{}, false, fmt.Errorf("more than one active job")
		}
		copy := candidate
		active = &copy
	}
	if active == nil {
		return Job{}, Plan{}, false, nil
	}
	plan, ok := state.Plans[active.PlanTokenHash]
	if !ok {
		return Job{}, Plan{}, false, fmt.Errorf("active job plan is missing")
	}
	return *active, plan, true, nil
}

func (engine *JobEngine) updateJob(ctx context.Context, jobID string, mutate func(*Job, *RuntimeState)) error {
	return engine.store.Update(ctx, func(state *RuntimeState) error {
		job, ok := state.Jobs[jobID]
		if !ok {
			return fmt.Errorf("job %s no longer exists", jobID)
		}
		mutate(&job, state)
		job.UpdatedAt = engine.now().UTC()
		state.Jobs[jobID] = job
		return nil
	})
}

func countActiveJobs(state RuntimeState) int {
	count := 0
	for _, job := range state.Jobs {
		if job.Status.active() {
			count++
		}
	}
	return count
}
