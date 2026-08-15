package poller_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uloydev/poller"
)

func TestMemoryStore_ListDueReturnsDueJobsSorted(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		jobs []poller.Job
		want []string
	}{
		{
			name: "due and not due",
			jobs: []poller.Job{
				{ID: "old", Next: now.Add(-time.Minute), Interval: time.Second},
				{ID: "late", Next: now.Add(time.Minute), Interval: time.Second},
				{ID: "exact", Next: now, Interval: time.Second},
			},
			want: []string{"old", "exact"},
		},
		{
			name: "all due",
			jobs: []poller.Job{
				{ID: "b", Next: now.Add(-time.Minute), Interval: time.Second},
				{ID: "a", Next: now.Add(-2 * time.Minute), Interval: time.Second},
			},
			want: []string{"a", "b"},
		},
		{
			name: "none due",
			jobs: []poller.Job{
				{ID: "future", Next: now.Add(time.Second), Interval: time.Second},
			},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := poller.NewMemoryStore()
			for _, job := range tt.jobs {
				store.Add(job)
			}

			due, err := store.ListDue(ctx, now)

			require.NoError(t, err)
			got := make([]string, 0, len(due))
			for _, job := range due {
				got = append(got, job.ID)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMemoryStore_ListDueReturnsNilOnCanceledContext(t *testing.T) {
	store := poller.NewMemoryStore()
	store.Add(poller.Job{ID: "a", Next: time.Now(), Interval: time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := store.ListDue(ctx, time.Now())

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestMemoryStore_SetNextAdvancesJob(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	store := poller.NewMemoryStore()
	store.Add(poller.Job{ID: "a", Next: now, Interval: time.Second})

	next := now.Add(time.Minute)
	require.NoError(t, store.SetNext(ctx, poller.Job{ID: "a"}, next))

	due, err := store.ListDue(ctx, now)
	require.NoError(t, err)
	assert.Empty(t, due, "job advanced past now must not be due")

	due, err = store.ListDue(ctx, next)
	require.NoError(t, err)
	require.Len(t, due, 1)
	assert.Equal(t, next, due[0].Next)
}

func TestMemoryStore_SetNextUnknownJob(t *testing.T) {
	err := poller.NewMemoryStore().SetNext(context.Background(), poller.Job{ID: "missing"}, time.Now())

	require.Error(t, err)
	assert.ErrorIs(t, err, poller.ErrJobNotFound)
}

func TestMemoryStore_SetNextReturnsNilOnCanceledContext(t *testing.T) {
	store := poller.NewMemoryStore()
	store.Add(poller.Job{ID: "a", Next: time.Now(), Interval: time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.SetNext(ctx, poller.Job{ID: "a"}, time.Now())

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestMemoryQueue_ConsumeDeliversInOrder(t *testing.T) {
	ctx := context.Background()
	queue := poller.NewMemoryQueue()

	delivered := make(chan string, 3)
	done := make(chan error, 1)
	go func() {
		done <- queue.Consume(ctx, func(_ context.Context, job poller.Job) {
			delivered <- job.ID
		})
	}()

	for _, id := range []string{"a", "b", "c"} {
		require.NoError(t, queue.Publish(ctx, poller.Job{ID: id}))
	}

	ids := make([]string, 0, 3)
	for range 3 {
		select {
		case id := <-delivered:
			ids = append(ids, id)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for job delivery")
		}
	}
	assert.Equal(t, []string{"a", "b", "c"}, ids)

	queue.Close()
	require.NoError(t, <-done)
}

func TestMemoryQueue_ConsumeStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	queue := poller.NewMemoryQueue()

	done := make(chan error, 1)
	go func() {
		done <- queue.Consume(ctx, func(context.Context, poller.Job) {})
	}()

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Consume did not return after context cancel")
	}
}

func TestMemoryQueue_CloseStopsConsuming(t *testing.T) {
	ctx := context.Background()
	queue := poller.NewMemoryQueue()

	done := make(chan error, 1)
	go func() {
		done <- queue.Consume(ctx, func(context.Context, poller.Job) {})
	}()

	queue.Close()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Consume did not return after Close")
	}
}

func TestMemoryQueue_PublishAfterClose(t *testing.T) {
	queue := poller.NewMemoryQueue()
	queue.Close()

	err := queue.Publish(context.Background(), poller.Job{ID: "a"})

	require.Error(t, err)
	assert.ErrorIs(t, err, poller.ErrQueueClosed)
}

func TestMemoryQueue_CloseIsIdempotent(t *testing.T) {
	queue := poller.NewMemoryQueue()

	assert.NotPanics(t, func() {
		queue.Close()
		queue.Close()
	})
}

func TestMemorySink_StoresResultsPerEntity(t *testing.T) {
	ctx := context.Background()
	sink := poller.NewMemorySink()
	result := poller.Result{PolledAt: time.Now(), Metrics: []poller.Metric{{Name: "demo_value_percent", Value: 42}}}

	require.NoError(t, sink.Write(ctx, "dev-1", result))
	require.NoError(t, sink.Write(ctx, "dev-2", result))
	require.NoError(t, sink.Write(ctx, "dev-1", result))

	got := sink.Results("dev-1")
	require.Len(t, got, 2)
	assert.Equal(t, []string{"dev-1", "dev-2"}, sink.Entities())
}

func TestMemorySink_ResultsReturnsCopy(t *testing.T) {
	sink := poller.NewMemorySink()
	result := poller.Result{Metrics: []poller.Metric{{Name: "demo_value_percent", Value: 1}}}
	require.NoError(t, sink.Write(context.Background(), "dev-1", result))

	sink.Results("dev-1")[0].Metrics[0].Value = 999

	got := sink.Results("dev-1")
	assert.InDelta(t, 1.0, got[0].Metrics[0].Value, 0)
}

func TestMemorySink_WriteReturnsNilOnCanceledContext(t *testing.T) {
	sink := poller.NewMemorySink()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sink.Write(ctx, "dev-1", poller.Result{})

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}
