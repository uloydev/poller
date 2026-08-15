// Command demo wires a poller.Scheduler, two poller.Workers, and a fake
// Collector together with the in-memory implementations and prints the
// collected metrics after a fixed run.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/uloydev/poller"
)

func main() {
	duration := flag.Duration("duration", 5*time.Second, "how long to run")
	entities := flag.Int("entities", 3, "number of entities to poll")
	interval := flag.Duration("interval", time.Second, "poll interval per entity")
	jitter := flag.Duration("jitter", 200*time.Millisecond, "maximum scheduling jitter per entity")
	concurrency := flag.Int("concurrency", 4, "concurrent collections per worker")
	seed := flag.Int64("seed", 1, "seed for the deterministic random walk")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	store := poller.NewMemoryStore()
	queue := poller.NewMemoryQueue()
	sink := poller.NewMemorySink()
	collector := newFakeCollector(*seed)

	for i := range *entities {
		store.Add(poller.Job{
			ID:       fmt.Sprintf("dev-%d", i),
			Next:     time.Now(),
			Interval: *interval,
			Jitter:   *jitter,
		})
	}

	scheduler, err := poller.NewScheduler(poller.SchedulerConfig{
		Store:      store,
		Publisher:  queue,
		JitterFunc: poller.UniformJitter,
		Logger:     logger,
	})
	if err != nil {
		logger.Error("create scheduler", "error", err)
		os.Exit(1)
	}

	workerCfg := poller.WorkerConfig{
		Consumer:    queue,
		Collector:   collector,
		Sink:        sink,
		Concurrency: *concurrency,
		Logger:      logger,
	}
	workerA, err := poller.NewWorker(workerCfg)
	if err != nil {
		logger.Error("create worker a", "error", err)
		os.Exit(1)
	}
	workerB, err := poller.NewWorker(workerCfg)
	if err != nil {
		logger.Error("create worker b", "error", err)
		os.Exit(1)
	}

	schedCtx, schedCancel := context.WithCancel(context.Background())
	schedDone := make(chan struct{})
	go func() {
		defer close(schedDone)
		if err := scheduler.Run(schedCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("scheduler stopped", "error", err)
		}
	}()

	workerDone := make(chan error, 2)
	for _, w := range []*poller.Worker{workerA, workerB} {
		go func() { workerDone <- w.Run(context.Background()) }()
	}

	logger.Info("demo running",
		"duration", *duration,
		"entities", *entities,
		"interval", *interval,
		"jitter", *jitter,
		"concurrency", *concurrency,
	)
	time.Sleep(*duration)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	for _, w := range []*poller.Worker{workerA, workerB} {
		if err := w.Stop(stopCtx); err != nil {
			logger.Error("stop worker", "error", err)
		}
	}
	stopCancel()
	for range 2 {
		if err := <-workerDone; err != nil {
			logger.Error("worker stopped", "error", err)
		}
	}

	schedCancel()
	<-schedDone

	printResults(sink)
}

func printResults(sink *poller.MemorySink) {
	for _, id := range sink.Entities() {
		results := sink.Results(id)
		if len(results) == 0 {
			continue
		}
		last := results[len(results)-1]
		fmt.Printf("%s: %d polls, latest %s\n", id, len(results), last.PolledAt.Format(time.RFC3339Nano))
		for _, m := range last.Metrics {
			fmt.Printf("  %s = %.2f\n", m.Name, m.Value)
		}
	}
}

// fakeCollector walks a seeded random value per entity. The walk is
// deterministic for a fixed seed, so re-running the demo reproduces the same
// series.
type fakeCollector struct {
	mu    sync.Mutex
	state map[string]*walk
	seed  uint64
}

type walk struct {
	mu    sync.Mutex
	value float64
	step  int
	rng   *rand.Rand
}

func newFakeCollector(seed int64) *fakeCollector {
	return &fakeCollector{
		state: make(map[string]*walk),
		seed:  uint64(seed),
	}
}

func (c *fakeCollector) Collect(ctx context.Context, entityID string) (poller.Result, error) {
	if err := ctx.Err(); err != nil {
		return poller.Result{}, err
	}
	c.mu.Lock()
	w := c.state[entityID]
	if w == nil {
		h := fnv.New64a()
		_, _ = h.Write([]byte(entityID))
		w = &walk{
			value: 50,
			//nolint:gosec // fake walk for the demo; not security-sensitive
			rng: rand.New(rand.NewPCG(h.Sum64(), c.seed)),
		}
		c.state[entityID] = w
	}
	c.mu.Unlock()

	return poller.Result{
		PolledAt: time.Now(),
		Metrics: []poller.Metric{
			{Name: "demo_value_percent", Value: w.next()},
			{Name: "demo_polls", Value: w.polls()},
		},
	}, nil
}

func (w *walk) next() float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.step++
	w.value += w.rng.NormFloat64() * 3
	w.value = min(max(w.value, 0), 100)
	return w.value
}

func (w *walk) polls() float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return float64(w.step)
}
