# Metric Conventions

This document defines how collectors in this repository name, unit, and label
their metrics. It applies to any `poller.Collector` implementation.

## Naming

`<vendor>_<name>`, snake_case, lowercase. `vendor` identifies the source of
the metric — the device vendor, service, or subsystem being polled. It is
fixed per collector and must not vary between polls.

Examples: `starlink_signal_quality`, `mikrotik_if_in_octets`, `demo_value_percent`.

## Units

The unit is mandatory as a name suffix for durations, counters, rates, and
ratios:

| Suffix | Meaning |
|--------|---------|
| `_s` | seconds |
| `_ms` | milliseconds |
| `_bytes` | cumulative bytes |
| `_bps` | bits per second |
| `_percent` | 0–100 ratio |

Plain numeric metrics carry no suffix; the unit lives in the catalog (see
below). Do not invent new suffixes for existing units.

## Labels

- Interface-scoped metrics use `labels{interface=<ifname>}`.
- The entity being polled is **not** a label — it is the job ID and is carried
  out of band by the framework (`entityID` in `Collect`/`Write`).
- Label values must not grow per poll: no timestamps, no random values, no
  per-request identifiers. Cardinality is fixed by the set of label names and
  the bounded set of label values.

## Timestamps

The collection timestamp (`Result.PolledAt`) is the time the value was read.
Collectors set it; sinks and downstream stores treat it as the metric
timestamp and may reorder on it.

## Counters

Counter metrics are monotonic. Consumers compute rates; writers never reset
or pre-diff a counter.

## Catalog

Every collector ships a catalog table listing its metric names, units, and
labels, like this one for the demo collector:

| Metric | Unit | Labels |
|--------|------|--------|
| `demo_value_percent` | percent 0–100 | — |
| `demo_polls` | monotonic count | — |

## Adding a metric

1. Add a row to the collector's catalog first.
2. Emit the metric from the Collector as `(name, value, labels)`.
3. Reference the exact name from the catalog anywhere downstream (alerts,
   dashboards, storage).
