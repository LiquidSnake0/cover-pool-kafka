# Cover Pool — a Kafka event pipeline in .NET

A producer emits mortgage loan events. A consumer replays them to maintain the
state of a **cover pool**: loan-to-value, eligibility, pool total.

Built to understand Kafka properly rather than to have read about it. The domain
is covered bonds because that is where ordering and idempotence stop being
abstract: a wrong number here is a wrong number in a regulatory report.

---

# Step 0 — Kafka, starting from what you already know

Three misconceptions to clear before touching the code.

### Kafka is not a queue

With a queue — MSMQ, RabbitMQ — you put a message in, somebody takes it out,
**it is gone**. Kafka does not do that.

**A Kafka topic is an append-only log that reading does not empty.** The message
stays. Ten different consumers can read the same message. You can go back and
read it all again tomorrow.

The closest thing you already know: **the SQL Server transaction log.** A
sequence of operations written in order, retained, replayable to rebuild a
state. That is the mental model.

### An offset is a bookmark, not a queue pointer

Each consumer records where it got to: that is the **offset**. It is stored per
**consumer group**, not per message. Two teams reading the same topic have two
independent bookmarks.

Resetting the offset to zero re-reads everything. That is a feature, not an
incident.

### Partitions are the log cut into pieces

A topic with 3 partitions is 3 independent log files. Why? Because one file can
only be read sequentially — three can be read in parallel.

**The price: ordering is only guaranteed within a partition.** Never across the
topic.

Which brings us to the only idea in this project that really matters.

---

# Step 1 — The key decides everything

When you produce a message you supply a **key**. Kafka hashes it to pick the
partition — exactly the way a `Dictionary<K,V>` picks a bucket.

```
hash("CH-0001") % 3  ->  partition 0     <- always the same
hash("CH-0006") % 3  ->  partition 1
```

**So: same key, same partition, ordering guaranteed for that key.**

Take CH-0001 in this project:

```
Originated  400,000 against a property worth 500,000  ->  LTV 80.0%  -> in
Revalued    the property falls to 450,000             ->  LTV 88.9%  -> out
Repaid      50,000 paid down                          ->  LTV 77.8%  -> back in
```

Those three events only make sense in that order. Replayed backwards the loan
ends up eligible when it should not be — and a wrong figure goes to the
regulator.

**Without a key, Kafka spreads messages round-robin and that ordering is gone.**
This is the line to remember from the whole project:

```csharp
Key = loanEvent.LoanId,
```

---

# Step 2 — Start the broker

```bash
cd cover-pool-kafka
./kafka-up.sh
```

Kafka wants roughly 1 GB of RAM. Close your browser first if the machine is
tight.

The script runs Kafka in **KRaft** mode — the modern setup, no Zookeeper. If you
find documentation talking about Zookeeper, it predates 2022.

It also creates the topic with **3 partitions** and prints:

```
Topic: loan-events   PartitionCount: 3   ReplicationFactor: 1
    Partition: 0    Leader: 1    Replicas: 1    Isr: 1
    Partition: 1    Leader: 1    Replicas: 1    Isr: 1
    Partition: 2    Leader: 1    Replicas: 1    Isr: 1
```

**ReplicationFactor: 1** — one copy. In production this would be 3, spread over
three brokers. **Isr** is *in-sync replicas*: the ones that are up to date.

---

# Step 3 — The producer

`src/CoverPool.Producer/Program.cs`

Three settings, all three worth being able to defend:

```csharp
Acks = Acks.All,
```
Only acknowledge once the message is written to **every** in-sync replica. With
`Acks.Leader` you are confirmed as soon as the leader has it — and if the leader
dies before replication, the message is lost. In a bank, `All`.

```csharp
EnableIdempotence = true,
```
The producer numbers its messages. If the network drops after the write but
before the acknowledgement, the client retries — and **without this setting the
message is written twice**. With it, the broker recognises the sequence number
and discards the duplicate.

```csharp
producer.Flush(TimeSpan.FromSeconds(10));
```
The client batches messages in an internal buffer. Exiting without `Flush` loses
whatever has not left yet. **Classic trap.**

### Run it

```bash
dotnet run --project src/CoverPool.Producer
```

```
-> CH-0001    Originated  partition 0  offset 0
-> CH-0001    Revalued    partition 0  offset 1
-> CH-0001    Repaid      partition 0  offset 2
-> CH-0006    Originated  partition 1  offset 0
-> CH-0008    Originated  partition 2  offset 0
```

**Look carefully:** all three CH-0001 events are on partition 0. CH-0006 is on
1, CH-0008 on 2. Offsets restart from 0 in each partition — they are local to
the partition, not global to the topic.

---

# Step 4 — The consumer

`src/CoverPool.Consumer/Program.cs`

```csharp
GroupId = "cover-pool-builder",
```
The **group**. Every consumer sharing this name splits the partitions between
them, and **each partition is read by exactly one of them**. Direct consequence:
parallelism is capped by the partition count. Ten consumers against three
partitions leaves seven idle.

```csharp
AutoOffsetReset = AutoOffsetReset.Earliest,
```
Where to start when the group has never read this topic. `Earliest` is from the
beginning of the log, `Latest` is only what arrives afterwards. It applies **only**
the first time; after that the committed offset wins.

```csharp
EnableAutoCommit = false,
...
consumer.Commit(result);   // AFTER processing
```

This is the most important decision in the file. The offset is committed
**after** the message is processed.

- Commit **after** processing — if you crash in between, the message is read
  again on restart. That is **at-least-once**: nothing lost, duplicates possible.
- Commit **before** processing — if you crash, the message is lost for good.
  That is **at-most-once**.

In a bank you do not lose messages. So at-least-once, **so duplicates**, so:

```csharp
var alreadySeen = new HashSet<string>();
...
if (!alreadySeen.Add(loanEvent.EventId)) { /* skipped */ }
```

**Idempotence.** And it is not theoretical here: `Repaid` carries a **delta**,
not an absolute value.

```csharp
OutstandingPrincipal -= e.RepaymentAmount ?? 0;
```

Replayed twice, the repayment subtracts twice, outstanding principal is wrong,
the LTV is wrong, eligibility is wrong. **A duplicate becomes a wrong figure in
a regulatory report.**

### Run it

In a second terminal, with Kafka running:

```bash
dotnet run --project src/CoverPool.Consumer
```

```
[C1] partitions assigned: 0, 1, 2

[C1] p0 o0   CH-0001  Originated  IN   LTV 80.0%
        `- pool: 7 eligible loans, 3,022,760 CHF - 1 excluded
[C1] p0 o1   CH-0001  Revalued    OUT  LTV 88.9% > 80%
        `- pool: 6 eligible loans, 2,622,760 CHF - 2 excluded
[C1] p0 o2   CH-0001  Repaid      IN   LTV 77.8%
        `- pool: 7 eligible loans, 2,972,760 CHF - 1 excluded
```

One consumer, so it gets all three partitions.

---

# Step 5 — Four experiments that prove you understood

These are what you get asked to describe, not the code.

## 5.1 — Replay the log from zero

```bash
KAFKA_GROUP=trial-$RANDOM dotnet run --project src/CoverPool.Consumer
```

A new group name is a new bookmark, so everything is read from the start and the
pool state rebuilds identically.

**What it demonstrates:** data is not consumed, it is retained. The state is a
**projection** of the log, not the source of truth. If the eligibility rules
change tomorrow, you replay everything under the new rules.

## 5.2 — Rebalancing

Leave the first consumer running. In a third terminal:

```bash
CONSUMER_NAME=C2 dotnet run --project src/CoverPool.Consumer
```

Watch both terminals:

```
[C1] partitions revoked: 0, 1, 2
[C1] partitions assigned: 0, 1
[C2] partitions assigned: 2
```

**That is a rebalance.** Note that consumption **stops** while it happens —
which is why unstable groups are a problem.

Start a third consumer: one partition each. A fourth: it sits idle.
**Parallelism is capped by the partition count.**

## 5.3 — Kill a consumer

Ctrl+C on C2. C1 picks its partitions back up within seconds.

The code calls `consumer.Close()` in the `finally` block, which tells the group
immediately. Without it you wait for the session to expire
(`session.timeout.ms`, 45 seconds by default) before Kafka notices the consumer
is gone.

## 5.4 — Duplicates

Kill a consumer with `kill -9` mid-batch, then restart it on the same group.
Messages processed but not committed come back, and you see:

```
[C1] duplicate ignored: a3f9c21e0b44 (CH-0001)
```

---

# Step 6 — Stop

```bash
./kafka-down.sh            # stop, keep the log
./kafka-down.sh --purge    # remove everything
```

---

# What is tested

14 tests, no broker, no network, 50 ms. The eligibility rules and the projection
are pure functions, so they are directly testable.

```bash
dotnet test
```

Two of them are worth reading, because they document reasoning rather than
verify code:

- `Replaying_a_repayment_subtracts_twice` — **documents the flaw, does not fix
  it.** A repayment is a delta; replayed, it subtracts twice. It is the
  justification for deduplicating in the consumer.
- `Replaying_a_revaluation_is_harmless` — the mirror image: an absolute value is
  naturally idempotent. The general rule lives in that pair.

---

## Layout

```
src/CoverPool.Contracts/    LoanEvent — what travels on the topic
src/CoverPool.Producer/     event emission, key = LoanId
src/CoverPool.Consumer/     pool projection, eligibility rules, deduplication
tests/CoverPool.Tests/      14 tests, no broker required
kafka-up.sh / kafka-down.sh single-node Kafka in KRaft, topic with 3 partitions
```

## What this does not do

No persistence — the projection is in memory and starts from scratch each run.
No schema registry: plain JSON, so nothing stops a producer breaking the
contract. No dead-letter handling for unprocessable messages. One broker, so no
fault tolerance at all.

It is a demonstration project built over a weekend, and that is what it should
be called.

## Licence

MIT
