package poller_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uloydev/poller"
)

type collectFunc func(context.Context, string) (poller.Result, error)

func (f collectFunc) Collect(ctx context.Context, entityID string) (poller.Result, error) {
	return f(ctx, entityID)
}

type consumeFunc func(context.Context, func(context.Context, poller.Job)) error

func (f consumeFunc) Consume(ctx context.Context, handler func(context.Context, poller.Job)) error {
	return f(ctx, handler)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func deliverAndBlock(jobs ...poller.Job) poller.Consumer {
	return consumeFunc(func(ctx context.Context, handler func(context.Context, poller.Job)) error {
		for _, job := range jobs {
			handler(ctx, job)
		}
		<-ctx.Done()
		return nil
	})
}

func waitRun(t *testing.T, done <-chan error, want error) {
	t.Helper()
	select {
	case err := <-done:
		require.ErrorIs(t, err, want)
	case <-time.After(5 * time.Second):
		t.Fatal("worker Run did not return")
	}
}

func TestNewWorker_ValidatesConfig(t *testing.T) {
	consumer := consumeFunc(func(context.Context, func(context.Context, poller.Job)) error { return nil })
	collector := collectFunc(func(context.Context, string) (poller.Result, error) { return poller.Result{}, nil })
	sink := poller.NewMemorySink()

	tests := []struct {
		name string
		cfg  poller.WorkerConfig
	}{
		{name: "nil consumer", cfg: poller.WorkerConfig{Collector: collector, Sink: sink}},
		{name: "nil collector", cfg: poller.WorkerConfig{Consumer: consumer, Sink: sink}},
		{name: "nil sink", cfg: poller.WorkerConfig{Consumer: consumer, Collector: collector}},
		{name: "negative concurrency", cfg: poller.WorkerConfig{Consumer: consumer, Collector: collector, Sink: sink, Concurrency: -1}},
		{name: "negative timeout", cfg: poller.WorkerConfig{Consumer: consumer, Collector: collector, Sink: sink, Timeout: -1}},
		{name: "negative max retries", cfg: poller.WorkerConfig{Consumer: consumer, Collector: collector, Sink: sink, MaxRetries: -1}},
		{name: "negative base delay", cfg: poller.WorkerConfig{Consumer: consumer, Collector: collector, Sink: sink, BaseDelay: -1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := poller.NewWorker(tt.cfg)
			require.Error(t, err)
		})
	}
}

func TestWorker_CollectsAndWrites(t *testing.T) {
	sink := poller.NewMemorySink()
	collector := collectFunc(func(ctx context.Context, entityID string) (poller.Result, error) {
		return poller.Result{
			PolledAt: time.Now(),
			Metrics:  []poller.Metric{{Name: "demo_value_percent", Value: 42}},
		}, nil
	})

	w, err := poller.NewWorker(poller.WorkerConfig{
		Consumer:  deliverAndBlock(poller.Job{ID: "dev-1"}),
		Collector: collector,
		Sink:      sink,
		Backoff:   func(int) time.Duration { return 0 },
		Logger:    discardLogger(),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	require.Eventually(t, func() bool {
		return len(sink.Results("dev-1")) == 1
	}, 5*time.Second, time.Millisecond)

	results := sink.Results("dev-1")
	require.Len(t, results, 1)
	require.Len(t, results[0].Metrics, 1)
	assert.Equal(t, "demo_value_percent", results[0].Metrics[0].Name)
	assert.InDelta(t, 42.0, results[0].Metrics[0].Value, 0)

	cancel()
	waitRun(t, runDone, context.Canceled)
}

func TestWorker_RespectsConcurrencyLimit(t *testing.T) {
	const (
		concurrency = 4
		jobs        = 12
	)

	release := make(chan struct{})
	var active, maxActive atomic.Int64
	collector := collectFunc(func(ctx context.Context, _ string) (poller.Result, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for {
			m := maxActive.Load()
			if n <= m || maxActive.CompareAndSwap(m, n) {
				break
			}
		}
		select {
		case <-release:
		case <-ctx.Done():
			return poller.Result{}, ctx.Err()
		}
		return poller.Result{PolledAt: time.Now(), Metrics: []poller.Metric{{Name: "demo_value_percent", Value: 1}}}, nil
	})

	jobList := make([]poller.Job, 0, jobs)
	for i := range jobs {
		jobList = append(jobList, poller.Job{ID: string(rune('a' + i))})
	}

	sink := poller.NewMemorySink()
	w, err := poller.NewWorker(poller.WorkerConfig{
		Consumer:    deliverAndBlock(jobList...),
		Collector:   collector,
		Sink:        sink,
		Concurrency: concurrency,
		Backoff:     func(int) time.Duration { return 0 },
		Logger:      discardLogger(),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	require.Eventually(t, func() bool { return active.Load() == concurrency },
		5*time.Second, time.Millisecond)
	assert.Equal(t, int64(concurrency), maxActive.Load(), "concurrent collections must never exceed the limit")

	close(release)
	require.Eventually(t, func() bool {
		return resultCount(sink) == jobs
	}, 5*time.Second, time.Millisecond)

	cancel()
	waitRun(t, runDone, context.Canceled)
	assert.Equal(t, int64(concurrency), maxActive.Load())
}

func resultCount(sink *poller.MemorySink) int {
	total := 0
	for _, id := range sink.Entities() {
		total += len(sink.Results(id))
	}
	return total
}

func TestWorker_AppliesPerJobTimeout(t *testing.T) {
	deadlineCh := make(chan time.Time, 1)
	collector := collectFunc(func(ctx context.Context, _ string) (poller.Result, error) {
		d, _ := ctx.Deadline()
		deadlineCh <- d
		return poller.Result{PolledAt: time.Now()}, nil
	})

	const timeout = 50 * time.Millisecond
	start := time.Now()

	w, err := poller.NewWorker(poller.WorkerConfig{
		Consumer:  deliverAndBlock(poller.Job{ID: "dev-1"}),
		Collector: collector,
		Sink:      poller.NewMemorySink(),
		Timeout:   timeout,
		Backoff:   func(int) time.Duration { return 0 },
		Logger:    discardLogger(),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	select {
	case deadline := <-deadlineCh:
		assert.WithinDuration(t, start.Add(timeout), deadline, 100*time.Millisecond)
	case <-time.After(5 * time.Second):
		t.Fatal("collection never started")
	}

	cancel()
	waitRun(t, runDone, context.Canceled)
}

func TestWorker_RetriesUntilSuccess(t *testing.T) {
	var calls atomic.Int64

	memSink := poller.NewMemorySink()
	collector := collectFunc(func(ctx context.Context, _ string) (poller.Result, error) {
		if calls.Add(1) < 3 {
			return poller.Result{}, errors.New("transient failure")
		}
		return poller.Result{PolledAt: time.Now(), Metrics: []poller.Metric{{Name: "demo_value_percent", Value: 7}}}, nil
	})

	w, err := poller.NewWorker(poller.WorkerConfig{
		Consumer:   deliverAndBlock(poller.Job{ID: "dev-1"}),
		Collector:  collector,
		Sink:       memSink,
		MaxRetries: 2,
		Backoff:    func(int) time.Duration { return 0 },
		Logger:     discardLogger(),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	require.Eventually(t, func() bool { return calls.Load() == 3 }, 5*time.Second, time.Millisecond)
	assert.Len(t, memSink.Results("dev-1"), 1, "one success must produce exactly one write")

	cancel()
	waitRun(t, runDone, context.Canceled)
}

func TestWorker_AppliesBackoffDelay(t *testing.T) {
	var calls atomic.Int64
	var attempts []int
	var mu sync.Mutex

	collector := collectFunc(func(ctx context.Context, _ string) (poller.Result, error) {
		if calls.Add(1) == 1 {
			return poller.Result{}, errors.New("transient failure")
		}
		return poller.Result{PolledAt: time.Now()}, nil
	})

	const delay = 25 * time.Millisecond
	w, err := poller.NewWorker(poller.WorkerConfig{
		Consumer:   deliverAndBlock(poller.Job{ID: "dev-1"}),
		Collector:  collector,
		Sink:       poller.NewMemorySink(),
		MaxRetries: 2,
		BaseDelay:  time.Second,
		Backoff: func(attempt int) time.Duration {
			mu.Lock()
			attempts = append(attempts, attempt)
			mu.Unlock()
			return delay
		},
		Logger: discardLogger(),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	require.Eventually(t, func() bool { return calls.Load() == 2 }, 5*time.Second, time.Millisecond)

	mu.Lock()
	gotAttempts := append([]int(nil), attempts...)
	mu.Unlock()
	assert.Equal(t, []int{1}, gotAttempts, "backoff must be applied before the first retry")
	assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond, "retry must wait for the backoff delay")

	cancel()
	waitRun(t, runDone, context.Canceled)
}

func TestWorker_GivesUpAfterMaxRetries(t *testing.T) {
	sentinel := errors.New("permanent failure")
	var calls atomic.Int64

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	collector := collectFunc(func(context.Context, string) (poller.Result, error) {
		calls.Add(1)
		return poller.Result{}, sentinel
	})

	w, err := poller.NewWorker(poller.WorkerConfig{
		Consumer:   deliverAndBlock(poller.Job{ID: "dev-1"}),
		Collector:  collector,
		Sink:       poller.NewMemorySink(),
		MaxRetries: 2,
		Backoff:    func(int) time.Duration { return 0 },
		Logger:     logger,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	require.Eventually(t, func() bool { return calls.Load() == 3 }, 5*time.Second, time.Millisecond)
	cancel()
	waitRun(t, runDone, context.Canceled)

	assert.Empty(t, poller.NewMemorySink().Results("dev-1"))
	assert.Contains(t, buf.String(), "after 3 attempts")
	assert.Contains(t, buf.String(), "permanent failure")
}

func TestWorker_DefaultBackoffIsExponentialWithJitter(t *testing.T) {
	var calls atomic.Int64
	sink := poller.NewMemorySink()
	collector := collectFunc(func(ctx context.Context, _ string) (poller.Result, error) {
		if calls.Add(1) <= 2 {
			return poller.Result{}, errors.New("transient failure")
		}
		return poller.Result{PolledAt: time.Now()}, nil
	})

	// Default backoff with a 20ms base delays by ~20ms before the first
	// retry and ~40ms before the second, each +-20% jitter.
	w, err := poller.NewWorker(poller.WorkerConfig{
		Consumer:   deliverAndBlock(poller.Job{ID: "dev-1"}),
		Collector:  collector,
		Sink:       sink,
		MaxRetries: 2,
		BaseDelay:  20 * time.Millisecond,
		Logger:     discardLogger(),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	require.Eventually(t, func() bool {
		return calls.Load() == 3 && len(sink.Results("dev-1")) == 1
	}, 5*time.Second, time.Millisecond)

	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 40*time.Millisecond,
		"retries must wait at least the base delay")
	assert.Less(t, elapsed, 110*time.Millisecond,
		"backoff must grow 1x, 2x, not faster")

	cancel()
	waitRun(t, runDone, context.Canceled)
}

func TestWorker_AbortsDuringBackoffOnCancel(t *testing.T) {
	var calls atomic.Int64
	collector := collectFunc(func(context.Context, string) (poller.Result, error) {
		calls.Add(1)
		return poller.Result{}, errors.New("boom")
	})

	w, err := poller.NewWorker(poller.WorkerConfig{
		Consumer:   deliverAndBlock(poller.Job{ID: "dev-1"}),
		Collector:  collector,
		Sink:       poller.NewMemorySink(),
		MaxRetries: 2,
		Backoff:    func(int) time.Duration { return time.Hour },
		Logger:     discardLogger(),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	require.Eventually(t, func() bool { return calls.Load() == 1 }, 5*time.Second, time.Millisecond)
	cancel()
	waitRun(t, runDone, context.Canceled)

	assert.Equal(t, int64(1), calls.Load(), "no retry may start after cancellation")
}

func TestWorker_StopsAndDrainsInFlight(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	sink := poller.NewMemorySink()

	collector := collectFunc(func(ctx context.Context, _ string) (poller.Result, error) {
		started <- struct{}{}
		select {
		case <-release:
			return poller.Result{PolledAt: time.Now(), Metrics: []poller.Metric{{Name: "demo_value_percent", Value: 1}}}, nil
		case <-ctx.Done():
			return poller.Result{}, ctx.Err()
		}
	})

	w, err := poller.NewWorker(poller.WorkerConfig{
		Consumer:  deliverAndBlock(poller.Job{ID: "dev-1"}),
		Collector: collector,
		Sink:      sink,
		Backoff:   func(int) time.Duration { return 0 },
		Logger:    discardLogger(),
	})
	require.NoError(t, err)

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(context.Background()) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("collection never started")
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release)
	}()

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	require.NoError(t, w.Stop(stopCtx), "Stop must wait for the in-flight collection")

	assert.Len(t, sink.Results("dev-1"), 1, "in-flight collection must complete before Stop returns")

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("worker Run did not return after Stop")
	}
}

func TestWorker_StopTimesOutOnHungCollection(t *testing.T) {
	never := make(chan struct{})
	started := make(chan struct{}, 1)
	collector := collectFunc(func(ctx context.Context, _ string) (poller.Result, error) {
		started <- struct{}{}
		<-never
		return poller.Result{}, nil
	})

	w, err := poller.NewWorker(poller.WorkerConfig{
		Consumer:  deliverAndBlock(poller.Job{ID: "dev-1"}),
		Collector: collector,
		Sink:      poller.NewMemorySink(),
		Backoff:   func(int) time.Duration { return 0 },
		Logger:    discardLogger(),
	})
	require.NoError(t, err)

	go func() { _ = w.Run(context.Background()) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("collection never started")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stopCancel()

	start := time.Now()
	err = w.Stop(stopCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
}

func TestWorker_ReturnsConsumerError(t *testing.T) {
	sentinel := errors.New("queue down")
	consumer := consumeFunc(func(context.Context, func(context.Context, poller.Job)) error {
		return sentinel
	})

	w, err := poller.NewWorker(poller.WorkerConfig{
		Consumer:  consumer,
		Collector: collectFunc(func(context.Context, string) (poller.Result, error) { return poller.Result{}, nil }),
		Sink:      poller.NewMemorySink(),
		Logger:    discardLogger(),
	})
	require.NoError(t, err)

	err = w.Run(context.Background())

	require.ErrorIs(t, err, sentinel)
	require.ErrorContains(t, err, "consume jobs")
}

func TestWorker_ReturnsNilWhenConsumerStopsCleanly(t *testing.T) {
	consumer := consumeFunc(func(context.Context, func(context.Context, poller.Job)) error {
		return nil
	})

	w, err := poller.NewWorker(poller.WorkerConfig{
		Consumer:  consumer,
		Collector: collectFunc(func(context.Context, string) (poller.Result, error) { return poller.Result{}, nil }),
		Sink:      poller.NewMemorySink(),
		Logger:    discardLogger(),
	})
	require.NoError(t, err)

	assert.NoError(t, w.Run(context.Background()))
}

func TestWorker_RecoversAfterTimedOutJob(t *testing.T) {
	sink := poller.NewMemorySink()
	collector := collectFunc(func(ctx context.Context, entityID string) (poller.Result, error) {
		if entityID == "good" {
			return poller.Result{PolledAt: time.Now(), Metrics: []poller.Metric{{Name: "demo_value_percent", Value: 1}}}, nil
		}
		<-ctx.Done()
		return poller.Result{}, ctx.Err()
	})

	w, err := poller.NewWorker(poller.WorkerConfig{
		Consumer:   deliverAndBlock(poller.Job{ID: "hung"}, poller.Job{ID: "good"}),
		Collector:  collector,
		Sink:       sink,
		Timeout:    20 * time.Millisecond,
		MaxRetries: 0,
		Backoff:    func(int) time.Duration { return 0 },
		Logger:     discardLogger(),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	require.Eventually(t, func() bool {
		return len(sink.Results("good")) == 1
	}, 5*time.Second, time.Millisecond)

	cancel()
	waitRun(t, runDone, context.Canceled)
}
