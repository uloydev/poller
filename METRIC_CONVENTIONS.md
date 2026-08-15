# Metric Conventions

## Naming

`<vendor>_<name>`, snake_case, lowercase. `vendor` is the `devices.vendor` value (`starlink`, `mikrotik`, `cisco`).

Examples: `starlink_signal_quality`, `mikrotik_if_in_octets`.

## Units

Unit is mandatory as a name suffix for durations, counters, rates, and ratios:

| Suffix | Meaning |
|--------|---------|
| `_s` | seconds |
| `_ms` | milliseconds |
| `_bytes` | cumulative bytes |
| `_bps` | bits per second |
| `_percent` | 0–100 ratio |

Plain numeric metrics carry no suffix; the unit lives in the catalog.

## Labels

- Interface-scoped metrics: `labels{interface=<ifname>}`
- Site and device are **not** labels — device is the `device_id` column; site comes from PostgreSQL
- Label values must not grow per poll (no timestamps, no random values) — cardinality control

## Timestamps

`polled_at` = the time the value was read on the device; fallback to receive time when the device reports none.

## Counters

Counter metrics are monotonic. Consumers compute rates; writers never reset or pre-diff.

## Initial catalog (M1)

### Starlink (gRPC)

| Metric | Unit | Labels |
|--------|------|--------|
| `starlink_signal_quality` | percent 0–100 | — |
| `starlink_uptime_s` | seconds | — |
| `starlink_rx_bytes` | cumulative bytes | — |
| `starlink_tx_bytes` | cumulative bytes | — |
| `starlink_latency_ms` | ms | — |
| `starlink_obstruction_percent` | percent 0–100 | — |
| `starlink_downlink_throughput_bps` | bps | — |
| `starlink_uplink_throughput_bps` | bps | — |

### SNMP gear (vendor-prefixed)

| Metric | Unit | Labels |
|--------|------|--------|
| `<vendor>_if_in_octets` | cumulative bytes | interface |
| `<vendor>_if_out_octets` | cumulative bytes | interface |
| `<vendor>_if_in_errors` | cumulative count | interface |
| `<vendor>_if_out_errors` | cumulative count | interface |
| `<vendor>_if_oper_status` | 1=up 2=down enum | interface |
| `<vendor>_cpu_percent` | percent 0–100 | — |
| `<vendor>_mem_percent` | percent 0–100 | — |
| `<vendor>_uptime_s` | seconds | — |

## Adding a metric

1. Add a row to this catalog first
2. Map the raw field in the Transformer to `(name, value, labels)`
3. Alert rules reference the exact name from the catalog
