// Package poller is a polling framework: schedule work due at intervals, fan
// it out across a bounded worker pool, collect values through a Collector,
// and write results to a Sink.
//
// The framework is split into two halves. A Scheduler advances jobs stored in
// a Store and publishes each due job through a Publisher — only when this
// instance is the elected leader — spreading load with per-job jitter.
// Workers consume those jobs from a queue (Consumer), collect metrics through
// the Collector, retry failures with exponential backoff and jitter, and
// write results to a Sink.
//
// Every collaborator is a small interface defined at the consumer side, so
// each half runs against whatever transport the caller provides. This
// package ships in-memory Store, queue, and Sink implementations for tests
// and demos; NATS implementations live in the natsq subpackage. Metric names,
// units, and labels follow METRIC_CONVENTIONS.md.
package poller
