# Cover Pool — a Kafka event pipeline in .NET

A producer emits mortgage loan events. A consumer replays them into a projection
of a **covered bond cover pool**: loan-to-value per loan, eligibility, pool total.

The domain is deliberate. Covered bonds are where message ordering and
idempotence stop being abstract properties — the output is a figure that goes
into a regulatory report, so getting either wrong is not a performance problem,
it is a wrong number.

```
[C1] p0 o0   CH-0001  Originated  IN   LTV 80.0%
        `- pool: 7 eligible loans, 3,022,760 CHF - 1 excluded
[C1] p0 o1   CH-0001  Revalued    OUT  LTV 88.9% > 80%
        `- pool: 6 eligible loans, 2,622,760 CHF - 2 excluded
[C1] p0 o2   CH-0001  Repaid      IN   LTV 77.8%
        `- pool: 7 eligible loans, 2,972,760 CHF - 1 excluded
```

---

## Quick start

Requirements: Docker and the .NET 10 SDK. Kafka needs roughly 1 GB of RAM.

```bash
./kafka-up.sh                                    # single-node Kafka, 3 partitions
dotnet run --project src/CoverPool.Producer      # emit the scenario
dotnet run --project src/CoverPool.Consumer      # build the pool projection
./kafka-down.sh                                  # stop, keeping the log
```

---

## The domain, briefly

A **covered bond** is backed by a ring-fenced pool of mortgages. A loan enters
the pool when it meets the eligibility criteria and leaves when it stops doing
so, which makes the pool dynamic rather than fixed at issuance.

The rules modelled here are a simplified version of the real ones:

| Rule | Reason for exclusion |
|---|---|
| LTV at or below 80% | `LTV 88.9% > 80%` |
| Not in default | `in default` |
| Currency is CHF | `currency EUR` |
| Outstanding principal above zero | `nothing outstanding` |
| Property has a valuation | `no valuation` |

Four event types drive the state: `Originated`, `Revalued`, `Repaid`,
`Defaulted`.

---

## Design decisions

### Events are keyed by loan ID

Kafka guarantees ordering **within a partition only**, and the partition is
chosen by hashing the message key. Keying by loan ID puts every event for one
loan on one partition, so that loan's history stays ordered.

This is a correctness requirement here, not a preference. Take CH-0001:

```
Originated  400,000 against a property worth 500,000  ->  LTV 80.0%  -> in
Revalued    the property falls to 450,000             ->  LTV 88.9%  -> out
Repaid      50,000 paid down                          ->  LTV 77.8%  -> back in
```

Processed out of order, the loan never leaves the pool, and the pool published
at that moment is wrong. Without a key Kafka distributes round-robin and that
ordering is gone.

### Offsets are committed after processing

`EnableAutoCommit = false`, and the commit happens once the event has been
applied. That is **at-least-once** delivery: nothing is lost if the process dies
mid-batch, but a message can be delivered twice.

Committing before processing would be at-most-once — a crash in between loses
the event permanently, which is not acceptable when the output is a regulatory
figure.

### At-least-once therefore requires idempotence

Duplicates are guaranteed, not hypothetical, so the consumer deduplicates on
event ID.

This matters unevenly across the event types, and the difference is the useful
part:

- `Repaid` carries a **delta** (`OutstandingPrincipal -= amount`). Applied
  twice, it subtracts twice, and the LTV and eligibility that follow are both
  wrong.
- `Revalued` carries an **absolute value**. Applied twice, it is a no-op.

Absolute events are naturally idempotent; deltas are not. Both cases are pinned
by tests.

### The pool is a projection, not a store

Nothing is persisted. The pool state is derived by replaying the log, so a
change to the eligibility rules does not require a data migration — a fresh
consumer group replays the same events under the new rules, while the existing
group carries on untouched.

### Producer settings

`Acks.All` — acknowledge only once every in-sync replica has the write.
`EnableIdempotence = true` — the producer sequences its messages so a retry
after a network timeout is discarded by the broker rather than written twice.

---

## Things to try

**Rebalancing.** With one consumer running, start a second:

```bash
CONSUMER_NAME=C2 dotnet run --project src/CoverPool.Consumer
```

Every assignment is revoked and the partitions are redistributed. Consumption
stops while this happens. A third consumer gets one partition each; a fourth
sits idle, because parallelism is capped by the partition count.

**Clean shutdown.** Stopping a consumer with Ctrl+C calls `consumer.Close()`,
which notifies the group immediately. Without it the group waits out
`session.timeout.ms` — 45 seconds by default — before reassigning.

**Deduplication.** With a consumer running:

```bash
dotnet run --project src/CoverPool.Producer -- --with-duplicate
```

One event is re-sent verbatim and the consumer reports `duplicate ignored`. The
replayed event is the CH-0001 repayment, the one where a replay would actually
corrupt the projection. Kafka produces such duplicates on its own after a crash
or rebalance; the flag only makes it repeatable.

**Replay from the beginning.**

```bash
KAFKA_GROUP=trial-$RANDOM dotnet run --project src/CoverPool.Consumer
```

A new group has no committed offset, so the whole log is read and the projection
rebuilds identically, without disturbing the existing group.

---

## Tests

14 tests, no broker and no network, around 50 ms.

```bash
dotnet test
```

The eligibility rules and the projection are pure functions, so they are
directly testable. Two tests document reasoning rather than verify behaviour:
`Replaying_a_repayment_subtracts_twice` records the delta problem without fixing
it, and `Replaying_a_revaluation_is_harmless` shows the contrast that justifies
deduplicating on event ID.

---

## Layout

```
src/CoverPool.Contracts/    LoanEvent — what travels on the topic
src/CoverPool.Producer/     event emission, key = LoanId
src/CoverPool.Consumer/     pool projection, eligibility rules, deduplication
tests/CoverPool.Tests/      14 tests, no broker required
kafka-up.sh / kafka-down.sh single-node Kafka in KRaft mode, 3 partitions
```

Kafka runs in KRaft mode, so there is no Zookeeper. Documentation that mentions
Zookeeper predates 2022.

---

## Limitations

Deliberate, because this is a demonstration rather than a system.

- **No persistence.** The projection is in memory and starts from scratch each
  run.
- **Deduplication is in-memory**, so it only protects within one process
  lifetime. A restart will re-apply events it saw before. In production this is
  a table keyed by event ID, written in the same transaction as the projection.
- **No schema registry.** Plain JSON, so nothing stops a producer breaking the
  contract.
- **No dead-letter handling** for unprocessable messages.
- **One broker**, replication factor 1, so no fault tolerance whatsoever.

## Licence

MIT
