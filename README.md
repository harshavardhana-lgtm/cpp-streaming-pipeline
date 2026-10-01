# Streaming Pipeline (C++11/14)

An in-process publish/subscribe pipeline with partitions, batching, consumer
groups, offset commits, and at-least-once delivery. Models the semantics you get
from RabbitMQ (work queues, manual ack, durable offsets) and Kafka (partitioned
append-only logs, consumer groups, offset commits) without needing a broker
running to demonstrate them.

## Model

- **Topics** partition onto a fixed number of partitions. Multiple topics can
  share a partition, so delivery filters by topic rather than assuming one topic
  per partition.
- **Offsets** are monotonic and assigned under the partition lock.
- **Consumer groups** track a *committed* position and an in-flight *next*
  cursor separately. That distinction is what makes at-least-once delivery real:
  a batch is delivered but not yet durable, and a crash rewinds the cursor to
  the last commit rather than skipping the record.
- **Batching** amortizes the per-record cost, bounded by a configurable maximum
  so a slow consumer cannot be handed an unbounded slice of memory.
- **Rewind** (`nack`) re-delivers the uncommitted batch, which is how you get
  redelivery after a processing failure.

The consumer tests cover offset monotonicity, batch limits, ack-gated
redelivery, topic isolation within a shared partition, partition mapping
stability, monotonic commit, rewind-and-replay, and a no-loss run over 5,000
records.

## Heterogeneous transform

A small normalizer maps differently delimited source records onto a common
field-tagged form (`k=`, `v1=`, `v2=`, …) regardless of the input delimiter.
This is the shape of a schema-migration stage sitting between an untrusted
feeder and a typed consumer.

```bash
./build/pipeline --transform
```

## Throughput

Publishing is dominated by payload construction here, so the numbers are
sandbox-relative:

| Scenario | Result |
| -------- | ------ |
| 200,000 records x 256 B | ~49 MB in ~0.8 s, 100% consumed |
| 128 MB stream, 1024 B payloads | 131,072 records published and consumed |

The interesting property is not the absolute rate — it is that consumed equals
published across partitions and topics, with no gaps and no duplicates beyond
the intended at-least-once semantics.

## Build and run

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
./build/pipeline --records 200000 --payload 256
./build/pipeline --stream-mb 128 --payload 1024
./build/pipeline --transform
ctest --test-dir build --output-on-failure
```

Options: `--records`, `--payload`, `--partitions`, `--batch`, `--consumers`,
`--stream-mb`, `--transform`.

## Layout

```
include/pipeline/broker.hpp  topics, partitions, groups, consumer, producer
src/broker.cpp               broker and consumer implementation
src/main.cpp                 CLI
tests/test_broker.cpp        unit tests
```

## Note on scope

The broker is a single-process, in-memory implementation. It exists to make the
delivery guarantees explicit and testable, not to be a drop-in RabbitMQ or Kafka
client. There is no disk durability, no replication, and no network protocol —
those are the broker's job in production. The value here is that the offset,
commit, and rewind logic is real and covered by tests.
