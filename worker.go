package poller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// BackoffFunc returns the delay to wait before retry attempt (1-based). The
// worker sleeps for this delay between a failed collection and the next
// attempt. A nil Backoff in WorkerConfig falls back to exponential backoff
// with +-20% jitter: base, 2*base, 4*base, and so on.
type BackoffFunc func(attempt int) time.Duration

// WorkerConfig configures a Worker. Consumer, Collector, and Sink are
// required; every other field has a default (see NewWorker).
type WorkerConfig struct {
	// Consumer delivers jobs from the queue. Required.
	Consumer Consumer
	// Collector reads metrics for each job. Required.
	Collector Collector
	// Sink receives the results of successful collections. Required.
	Sink Sink
	// Concurrency is the maximum number of collections running at once.
	// Defaults to 10.
	Concurrency int
	// Timeout bounds each collection and each sink write. Defaults to 10s.
	Timeout time.Duration
	// MaxRetries is the number of retries after the first attempt; a job
	// therefore runs at most MaxRetries+1 times. Defaults to 2.
	MaxRetries int
	// BaseDelay is the base of the default exponential backoff. Defaults
	// to 1s.
	BaseDelay time.Duration
	// Backoff overrides the retry schedule. Defaults to exponential
	// backoff with +-20% jitter over BaseDelay.
	Backoff BackoffFunc
	// Logger receives structured log output. Defaults to slog.Default().
	Logger *slog.Logger
}

// NewWorker validates cfg and returns a Worker with defaults applied.
// Consumer, Collector, and Sink must be set, and negative values for the
// numeric fields are rejected. Zero values fall back to the defaults.
func NewWorker(cfg WorkerConfig) (*Worker, error) {
	if cfg.Consumer == nil {
		return nil, errors.New("poller: worker requires a consumer")
	}
	if cfg.Collector == nil {
		return nil, errors.New("poller: worker requires a collector")
	}
	if cfg.Sink == nil {
		return nil, errors.New("poller: worker requires a sink")
	}
	if cfg.Concurrency < 0 {
		return nil, errors.New("poller: concurrency must not be negative")
	}
	if cfg.Timeout < 0 {
		return nil, errors.New("poller: timeout must not be negative")
	}
	if cfg.MaxRetries < 0 {
		return nil, errors.New("poller: max retries must not be negative")
	}
	if cfg.BaseDelay < 0 {
		return nil, errors.New("poller: base delay must not be negative")
	}

	concurrency := cfg.Concurrency
	if concurrency == 0 {
		concurrency = 10
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	maxRetries := cfg.MaxRetries
	if maxRetries == 0 {
		maxRetries = 2
	}
	baseDelay := cfg.BaseDelay
	if baseDelay == 0 {
		baseDelay = time.Second
	}
	backoff := cfg.Backoff
	if backoff == nil {
		backoff = exponentialBackoff(baseDelay)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		consumer:    cfg.Consumer,
		collector:   cfg.Collector,
		sink:        cfg.Sink,
		concurrency: concurrency,
		timeout:     timeout,
		maxRetries:  maxRetries,
		baseDelay:   baseDelay,
		backoff:     backoff,
		log:         logger,
	}, nil
}

// Worker consumes jobs from a queue and collects metrics for them. Each job
// is dispatched to one of a fixed pool of collectors, so at most
// Concurrency collections run at once. Every collection gets a per-job
// timeout, and failures are retried with exponential backoff until the job
// succeeds or MaxRetries is exhausted.
type Worker struct {
	consumer    Consumer
	collector   Collector
	sink        Sink
	concurrency int
	timeout     time.Duration
	maxRetries  int
	baseDelay   time.Duration
	backoff     BackoffFunc
	log         *slog.Logger

	mu            sync.Mutex
	running       bool
	cancelConsume context.CancelFunc
	runStopped    chan struct{}
	jobsCh        chan Job
	wg            sync.WaitGroup
}

// Run consumes jobs until ctx is canceled or the consumer fails, then waits
// for in-flight and queued collections to finish. It returns ctx.Err() on
// cancellation, a wrapped consumer error when the consumer fails, or nil when
// the consumer stops cleanly. Run is single-use: it returns an error if it is
// called while a previous Run has not returned.
func (w *Worker) Run(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return errors.New("poller: worker already running")
	}
	w.running = true
	stopped := make(chan struct{})
	w.runStopped = stopped
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.running = false
		w.mu.Unlock()
		close(stopped)
	}()

	consumeCtx, cancel := context.WithCancel(ctx)
	w.mu.Lock()
	w.cancelConsume = cancel
	w.mu.Unlock()
	defer cancel()

	w.jobsCh = make(chan Job, w.concurrency)
	w.wg.Add(w.concurrency)
	for range w.concurrency {
		go w.poolLoop(ctx)
	}

	err := w.consumer.Consume(consumeCtx, w.enqueue)
	close(w.jobsCh)
	w.wg.Wait()

	if err != nil {
		return fmt.Errorf("consume jobs: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// Stop stops consuming and waits for in-flight and queued collections to
// finish, bounded by ctx. On the way, pending jobs already handed to the
// worker are drained before the worker shuts down. It returns nil once the
// worker has fully stopped, or a wrapped ctx.Err() when the deadline expires
// first, which means at least one collection is still running. Calling Stop
// on a worker that has not been started is a no-op that returns nil.
func (w *Worker) Stop(ctx context.Context) error {
	w.mu.Lock()
	cancel := w.cancelConsume
	stopped := w.runStopped
	w.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if stopped == nil {
		return nil
	}
	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("drain in-flight collections: %w", ctx.Err())
	}
}

func (w *Worker) poolLoop(ctx context.Context) {
	defer w.wg.Done()
	for job := range w.jobsCh {
		if err := w.collect(ctx, job); err != nil {
			w.log.Error("collect job failed", "job", job.ID, "error", err)
		}
	}
}

func (w *Worker) enqueue(jobCtx context.Context, job Job) {
	select {
	case w.jobsCh <- job:
		return
	default:
		select {
		case w.jobsCh <- job:
		case <-jobCtx.Done():
		}
	}
}

func (w *Worker) collect(ctx context.Context, job Job) error {
	var lastErr error
	for attempt := 0; attempt <= w.maxRetries; attempt++ {
		if attempt > 0 {
			if err := w.waitForRetry(ctx, job, attempt); err != nil {
				return err
			}
		}
		result, err := w.attempt(ctx, job)
		if err != nil {
			lastErr = err
			w.log.Warn("collection failed", "job", job.ID, "attempt", attempt, "error", err)
			continue
		}
		if err := w.write(ctx, job, result); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("collect %s after %d attempts: %w", job.ID, w.maxRetries+1, lastErr)
}

func (w *Worker) attempt(ctx context.Context, job Job) (Result, error) {
	jobCtx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	return w.collector.Collect(jobCtx, job.ID)
}

func (w *Worker) write(ctx context.Context, job Job, result Result) error {
	writeCtx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	if err := w.sink.Write(writeCtx, job.ID, result); err != nil {
		return fmt.Errorf("write metrics for %s: %w", job.ID, err)
	}
	return nil
}

func (w *Worker) waitForRetry(ctx context.Context, job Job, attempt int) error {
	delay := w.backoff(attempt)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("canceled retry for %s: %w", job.ID, ctx.Err())
	}
}

func exponentialBackoff(base time.Duration) BackoffFunc {
	return func(attempt int) time.Duration {
		delay := base
		for i := 1; i < attempt; i++ {
			delay *= 2
		}
		jitter := delay / 5
		if jitter <= 0 {
			return delay
		}
		offset := time.Duration(rand.Int64N(2*int64(jitter) + 1)) //nolint:gosec // non-security randomness
		return delay - jitter + offset
	}
}
