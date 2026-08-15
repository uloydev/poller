// Package natsq provides NATS-backed Publisher and Consumer implementations
// for the poller framework. Jobs are encoded as JSON and routed on subjects
// of the form <prefix>.<job-id>; consumers subscribe to <prefix>.> inside a
// queue group, so the load is shared across workers.
package natsq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/uloydev/poller"
)

// chanBufSize is the buffer of the message channel behind a subscription.
const chanBufSize = 64

// Publisher publishes poller jobs to NATS on the subject <prefix>.<job-id>.
type Publisher struct {
	nc            *nats.Conn
	subjectPrefix string
}

// NewPublisher validates prefix and returns a Publisher that publishes jobs
// to the connection nc. An empty prefix defaults to "polls"; a prefix must be
// a single NATS subject token (no dots) so that the subject layout stays
// predictable. It returns an error when nc is nil or the prefix is invalid.
func NewPublisher(nc *nats.Conn, subjectPrefix string) (*Publisher, error) {
	prefix, err := normalizePrefix(subjectPrefix)
	if err != nil {
		return nil, err
	}
	if nc == nil {
		return nil, errors.New("natsq: publisher requires a NATS connection")
	}
	return &Publisher{nc: nc, subjectPrefix: prefix}, nil
}

// Publish encodes job as JSON and publishes it to <prefix>.<job-id>. It
// returns an error when ctx is canceled, the job cannot be encoded, or the
// publish fails.
func (p *Publisher) Publish(ctx context.Context, job poller.Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("encode job %s: %w", job.ID, err)
	}
	if err := p.nc.Publish(subject(p.subjectPrefix, job.ID), data); err != nil {
		return fmt.Errorf("publish job %s: %w", job.ID, err)
	}
	return nil
}

// Consumer receives jobs from a NATS queue group and hands them to a
// handler. Logger, when non-nil, receives structured log output; it defaults
// to slog.Default().
type Consumer struct {
	nc            *nats.Conn
	subjectPrefix string
	queueGroup    string

	Logger *slog.Logger
}

// NewConsumer validates prefix and queueGroup and returns a Consumer that
// receives jobs from the connection nc. An empty prefix defaults to "polls";
// queueGroup must be non-empty. It returns an error when nc is nil or the
// arguments are invalid.
func NewConsumer(nc *nats.Conn, subjectPrefix, queueGroup string) (*Consumer, error) {
	prefix, err := normalizePrefix(subjectPrefix)
	if err != nil {
		return nil, err
	}
	if queueGroup == "" {
		return nil, errors.New("natsq: consumer requires a queue group")
	}
	if nc == nil {
		return nil, errors.New("natsq: consumer requires a NATS connection")
	}
	return &Consumer{nc: nc, subjectPrefix: prefix, queueGroup: queueGroup}, nil
}

// Consume subscribes to <prefix>.> in the consumer's queue group and invokes
// handler for each job until ctx is canceled, then returns nil. Pending
// messages buffered in the subscription are still delivered before Consume
// returns. Implementations of poller.Consumer must not call handler after
// returning; this one never does, because handler runs in this goroutine.
func (c *Consumer) Consume(ctx context.Context, handler func(context.Context, poller.Job)) error {
	msgs := make(chan *nats.Msg, chanBufSize)
	sub, err := c.nc.ChanQueueSubscribe(subject(c.subjectPrefix, ">"), c.queueGroup, msgs)
	if err != nil {
		return fmt.Errorf("queue subscribe %s: %w", subject(c.subjectPrefix, ">"), err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	for {
		select {
		case <-ctx.Done():
			c.drain(msgs, handler)
			return nil
		case m := <-msgs:
			c.handle(m, handler)
		}
	}
}

func (c *Consumer) drain(msgs chan *nats.Msg, handler func(context.Context, poller.Job)) {
	for {
		select {
		case m := <-msgs:
			c.handle(m, handler)
		default:
			return
		}
	}
}

func (c *Consumer) handle(m *nats.Msg, handler func(context.Context, poller.Job)) {
	var job poller.Job
	if err := json.Unmarshal(m.Data, &job); err != nil {
		c.logger().Error("decode poll job", "subject", m.Subject, "error", err)
		return
	}
	handler(context.Background(), job)
}

func (c *Consumer) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

func normalizePrefix(prefix string) (string, error) {
	if prefix == "" {
		return "polls", nil
	}
	if strings.Contains(prefix, ".") {
		return "", errors.New("natsq: subject prefix must not contain dots")
	}
	return prefix, nil
}

func subject(prefix, key string) string {
	return prefix + "." + key
}
