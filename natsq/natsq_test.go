package natsq_test

import (
	"context"
	"encoding/json"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uloydev/poller"
	"github.com/uloydev/poller/natsq"
)

func startTestServer(t *testing.T) *nats.Conn {
	t.Helper()
	ns, err := server.NewServer(&server.Options{
		Host:   "127.0.0.1",
		Port:   server.RANDOM_PORT,
		NoLog:  true,
		NoSigs: true,
	})
	require.NoError(t, err)
	go ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "embedded NATS server did not start")

	nc, err := nats.Connect(ns.ClientURL(), nats.Timeout(2*time.Second))
	require.NoError(t, err)

	t.Cleanup(nc.Close)
	t.Cleanup(ns.Shutdown)
	return nc
}

func testJob(id string) poller.Job {
	return poller.Job{
		ID:       id,
		Next:     time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC),
		Interval: 30 * time.Second,
		Jitter:   2 * time.Second,
	}
}

func TestNewPublisher_ValidatesConfig(t *testing.T) {
	nc := startTestServer(t)

	tests := []struct {
		name   string
		nc     *nats.Conn
		prefix string
	}{
		{name: "nil connection", nc: nil, prefix: "polls"},
		{name: "prefix with dot", nc: nc, prefix: "a.b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := natsq.NewPublisher(tt.nc, tt.prefix)
			require.Error(t, err)
		})
	}
}

func TestNewConsumer_ValidatesConfig(t *testing.T) {
	nc := startTestServer(t)

	tests := []struct {
		name       string
		nc         *nats.Conn
		prefix     string
		queueGroup string
	}{
		{name: "nil connection", nc: nil, prefix: "polls", queueGroup: "workers"},
		{name: "empty queue group", nc: nc, prefix: "polls", queueGroup: ""},
		{name: "prefix with dot", nc: nc, prefix: "a.b", queueGroup: "workers"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := natsq.NewConsumer(tt.nc, tt.prefix, tt.queueGroup)
			require.Error(t, err)
		})
	}
}

func TestPublisher_PublishesJobToSubject(t *testing.T) {
	nc := startTestServer(t)

	received := make(chan *nats.Msg, 1)
	sub, err := nc.Subscribe("polls.dev-1", func(m *nats.Msg) {
		received <- m
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	publisher, err := natsq.NewPublisher(nc, "polls")
	require.NoError(t, err)

	job := testJob("dev-1")
	require.NoError(t, publisher.Publish(context.Background(), job))
	require.NoError(t, nc.Flush())

	select {
	case m := <-received:
		assert.Equal(t, "polls.dev-1", m.Subject)
		var got poller.Job
		require.NoError(t, json.Unmarshal(m.Data, &got))
		assert.Equal(t, job, got)
	case <-time.After(5 * time.Second):
		t.Fatal("job was not received on its subject")
	}
}

func TestQueueGroup_SharesLoadAcrossConsumers(t *testing.T) {
	nc := startTestServer(t)

	const (
		jobs      = 40
		consumers = 2
	)

	var delivered atomic.Int64
	done := make(chan struct{}, consumers)
	cancels := make([]context.CancelFunc, 0, consumers)
	for range consumers {
		consumer, err := natsq.NewConsumer(nc, "polls", "demo-workers")
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		go func() {
			_ = consumer.Consume(ctx, func(_ context.Context, _ poller.Job) {
				delivered.Add(1)
			})
			done <- struct{}{}
		}()
	}
	require.NoError(t, nc.Flush(), "wait for subscriptions to be registered")

	publisher, err := natsq.NewPublisher(nc, "polls")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := range jobs {
		require.NoError(t, publisher.Publish(ctx, testJob("dev-"+strconv.Itoa(i))))
	}
	require.NoError(t, nc.Flush())

	require.Eventually(t, func() bool { return delivered.Load() == jobs },
		10*time.Second, time.Millisecond)

	for _, cancel := range cancels {
		cancel()
	}
	for range consumers {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("consumer did not stop after context cancel")
		}
	}
}

func TestConsumer_ReturnsNilOnCanceledContext(t *testing.T) {
	nc := startTestServer(t)

	consumer, err := natsq.NewConsumer(nc, "polls", "workers")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NoError(t, consumer.Consume(ctx, func(context.Context, poller.Job) {}))
}

func TestConsumer_SkipsMalformedMessages(t *testing.T) {
	nc := startTestServer(t)

	received := make(chan poller.Job, 1)
	consumer, err := natsq.NewConsumer(nc, "polls", "workers")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Consume(ctx, func(_ context.Context, job poller.Job) {
			received <- job
		})
	}()
	require.NoError(t, nc.Flush(), "wait for subscription to be registered")

	require.NoError(t, nc.Publish("polls.dev-1", []byte("not a job")))
	p, err := natsq.NewPublisher(nc, "polls")
	require.NoError(t, err)
	require.NoError(t, p.Publish(context.Background(), testJob("dev-1")))
	require.NoError(t, nc.Flush())

	select {
	case job := <-received:
		assert.Equal(t, "dev-1", job.ID, "malformed message must not reach the handler")
	case <-time.After(5 * time.Second):
		t.Fatal("valid job was not received")
	}

	cancel()
	require.NoError(t, <-done)
}

func TestConsumer_StopsReceivingAfterCancel(t *testing.T) {
	nc := startTestServer(t)

	consumerA, err := natsq.NewConsumer(nc, "polls", "workers")
	require.NoError(t, err)
	consumerB, err := natsq.NewConsumer(nc, "polls", "workers")
	require.NoError(t, err)

	var receivedA atomic.Int64
	ctxA, cancelA := context.WithCancel(context.Background())
	doneA := make(chan error, 1)
	go func() {
		doneA <- consumerA.Consume(ctxA, func(_ context.Context, _ poller.Job) {
			receivedA.Add(1)
		})
	}()

	var receivedB atomic.Int64
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	doneB := make(chan error, 1)
	go func() {
		doneB <- consumerB.Consume(ctxB, func(_ context.Context, _ poller.Job) {
			receivedB.Add(1)
		})
	}()
	require.NoError(t, nc.Flush(), "wait for subscriptions to be registered")

	publisher, err := natsq.NewPublisher(nc, "polls")
	require.NoError(t, err)

	require.NoError(t, publisher.Publish(context.Background(), testJob("warmup")))
	require.NoError(t, nc.Flush())
	require.Eventually(t, func() bool { return receivedA.Load() == 1 || receivedB.Load() == 1 },
		10*time.Second, time.Millisecond)
	cancelA()
	require.NoError(t, <-doneA)

	const after = 20
	base := receivedB.Load()
	for i := range after {
		require.NoError(t, publisher.Publish(context.Background(), testJob("dev-"+strconv.Itoa(i))))
	}
	require.NoError(t, nc.Flush())

	require.Eventually(t, func() bool { return receivedB.Load() == base+after },
		10*time.Second, time.Millisecond)
}
