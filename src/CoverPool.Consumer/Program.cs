using System.Text.Json;
using Confluent.Kafka;
using CoverPool.Consumer;
using CoverPool.Contracts;

const string Topic = "loan-events";

var bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "localhost:9092";
var group = Environment.GetEnvironmentVariable("KAFKA_GROUP") ?? "cover-pool-builder";
var name = Environment.GetEnvironmentVariable("CONSUMER_NAME") ?? "C1";

var config = new ConsumerConfig
{
    BootstrapServers = bootstrap,

    // Every consumer sharing this group id splits the partitions between them.
    // Each partition is read by exactly one of them.
    GroupId = group,

    // Where to start when the group has never read this topic. Earliest is
    // from the beginning of the log, Latest is new messages only. It applies
    // on the first run only; after that the committed offset wins.
    AutoOffsetReset = AutoOffsetReset.Earliest,

    // Offsets are committed by hand, AFTER processing. That is what gives
    // at-least-once delivery: if the process dies between processing and
    // committing, the message is read again on restart.
    EnableAutoCommit = false,
};

using var consumer = new ConsumerBuilder<string, string>(config)
    .SetPartitionsAssignedHandler((_, partitions) =>
        Console.WriteLine($"\n[{name}] partitions assigned: " +
                          string.Join(", ", partitions.Select(p => p.Partition.Value)) + "\n"))
    .SetPartitionsRevokedHandler((_, partitions) =>
        Console.WriteLine($"\n[{name}] partitions revoked: " +
                          string.Join(", ", partitions.Select(p => p.Partition.Value)) + "\n"))
    .Build();

consumer.Subscribe(Topic);

// The projection: the pool state, derived from the events.
var pool = new Dictionary<string, LoanState>();

// Deduplication. Kafka delivers at least once, so after a crash or a rebalance
// you WILL see messages you have already processed. Without this guard a
// replayed repayment is subtracted twice.
var alreadySeen = new HashSet<string>();

var stopping = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; stopping.Cancel(); };

Console.WriteLine($"[{name}] group \"{group}\", waiting on \"{Topic}\"");
Console.WriteLine($"[{name}] Ctrl+C to stop\n");

try
{
    while (!stopping.IsCancellationRequested)
    {
        var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
        if (result is null) continue;

        var loanEvent = JsonSerializer.Deserialize<LoanEvent>(result.Message.Value);
        if (loanEvent is null) continue;

        if (!alreadySeen.Add(loanEvent.EventId))
        {
            Console.WriteLine($"[{name}] duplicate ignored: {loanEvent.EventId} ({loanEvent.LoanId})");
            consumer.Commit(result);
            continue;
        }

        if (!pool.TryGetValue(loanEvent.LoanId, out var loan))
            pool[loanEvent.LoanId] = loan = new LoanState { LoanId = loanEvent.LoanId };

        var wasEligible = loan.IsEligible;
        loan.Apply(loanEvent);

        var (eligible, reason) = EligibilityRules.Evaluate(loan);
        loan.IsEligible = eligible;

        var transition = (wasEligible, eligible) switch
        {
            (false, true) => "IN  ",
            (true, false) => "OUT ",
            _             => "    ",
        };

        Console.WriteLine(
            $"[{name}] p{result.Partition.Value} o{result.Offset.Value,-3} " +
            $"{loanEvent.LoanId,-8} {loanEvent.Type,-11} {transition} {reason}");

        // Commit after processing. Swapping these two lines would turn this
        // into at-most-once: a crash in between would lose the message for good.
        consumer.Commit(result);

        if (transition.Trim().Length > 0) Summarise(pool, name);
    }
}
catch (OperationCanceledException) { }
finally
{
    // Leaving cleanly triggers an immediate rebalance instead of making the
    // group wait for the session to time out.
    consumer.Close();
    Console.WriteLine($"\n[{name}] closed.");
    Summarise(pool, name);
}

static void Summarise(Dictionary<string, LoanState> pool, string name)
{
    var eligible = pool.Values.Where(p => p.IsEligible).ToList();
    var total = eligible.Sum(p => p.OutstandingPrincipal);
    var excluded = pool.Count - eligible.Count;

    Console.WriteLine(
        $"        `- pool: {eligible.Count} eligible loans, " +
        $"{total:N0} CHF - {excluded} excluded");
}
