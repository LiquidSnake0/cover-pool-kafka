using System.Text.Json;
using Confluent.Kafka;
using CoverPool.Contracts;

const string Topic = "loan-events";
var bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "localhost:9092";

var config = new ProducerConfig
{
    BootstrapServers = bootstrap,

    // Wait for acknowledgement from every in-sync replica. On a single broker
    // this changes nothing; in production it is the difference between "sent"
    // and "durably written".
    Acks = Acks.All,

    // The producer numbers its own messages, so a retry after a network
    // timeout is recognised and discarded by the broker. Without this, a
    // timeout followed by a retry writes the message twice.
    EnableIdempotence = true,
};

// Re-sends one event verbatim at the end, same EventId, so the consumer's
// deduplication can be observed on demand. Kafka would produce this on its own
// after a crash or a rebalance; forcing it just makes the demo repeatable.
var withDuplicate = args.Contains("--with-duplicate");

using var producer = new ProducerBuilder<string, string>(config).Build();

Console.WriteLine($"Producer connected to {bootstrap}, topic \"{Topic}\"\n");

var sent = new List<LoanEvent>();

foreach (var loanEvent in Scenario())
{
    sent.Add(loanEvent);

    var message = new Message<string, string>
    {
        // The one line that matters in this file.
        //
        // The key decides the partition. Same loan, same partition, so the
        // events for one loan stay ordered. Without a key Kafka spreads
        // messages round-robin, and the revaluation can be processed after
        // the repayment.
        Key = loanEvent.LoanId,
        Value = JsonSerializer.Serialize(loanEvent),
    };

    var result = await producer.ProduceAsync(Topic, message);

    Console.WriteLine(
        $"-> {loanEvent.LoanId,-10} {loanEvent.Type,-11} " +
        $"partition {result.Partition.Value}  offset {result.Offset.Value}");

    await Task.Delay(250);
}

if (withDuplicate)
{
    // The CH-0001 repayment: the one event where a replay actually corrupts
    // the projection, because it carries a delta rather than an absolute value.
    var replayed = sent.First(e => e.Type == LoanEventType.Repaid);

    var result = await producer.ProduceAsync(Topic, new Message<string, string>
    {
        Key = replayed.LoanId,
        Value = JsonSerializer.Serialize(replayed),
    });

    Console.WriteLine(
        $"\n-> {replayed.LoanId,-10} {replayed.Type,-11} " +
        $"partition {result.Partition.Value}  offset {result.Offset.Value}   " +
        $"REPLAY of event {replayed.EventId}");
}

// Drain the internal buffer before exiting. Without this Flush, a producer
// that stops right after ProduceAsync can lose whatever has not left yet.
producer.Flush(TimeSpan.FromSeconds(10));
Console.WriteLine("\nDone.");

static IEnumerable<LoanEvent> Scenario()
{
    var t = DateTimeOffset.UtcNow;

    // -- CH-0001: the ordering demonstration -----------------------------
    // These three only make sense in this order. Replayed backwards the loan
    // ends up eligible when it should not be.
    yield return LoanEvent.Originated("CH-0001", 400_000m, 500_000m, t);   // LTV 80.0% -> in
    yield return LoanEvent.Revalued("CH-0001", 450_000m, t.AddSeconds(1)); // LTV 88.9% -> out
    yield return LoanEvent.Repaid("CH-0001", 50_000m, t.AddSeconds(2));    // LTV 77.8% -> back in

    // -- CH-0002: default ------------------------------------------------
    yield return LoanEvent.Originated("CH-0002", 300_000m, 600_000m, t);   // LTV 50% -> in
    yield return LoanEvent.Defaulted("CH-0002", t.AddSeconds(3));          // out for good

    // -- CH-0003: rejected on arrival, LTV too high ----------------------
    yield return LoanEvent.Originated("CH-0003", 480_000m, 500_000m, t);   // LTV 96% -> never in

    // -- CH-0004: ineligible currency ------------------------------------
    yield return LoanEvent.Originated("CH-0004", 200_000m, 400_000m, t, "EUR");

    // -- Volume, to see the spread across partitions ---------------------
    var random = new Random(42);
    for (var i = 5; i <= 14; i++)
    {
        var value = random.Next(400, 1_200) * 1_000m;
        var ltv = random.Next(45, 95) / 100m;
        yield return LoanEvent.Originated(
            $"CH-{i:D4}", Math.Round(value * ltv), value, t.AddSeconds(i));
    }
}
