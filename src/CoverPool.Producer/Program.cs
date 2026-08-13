using System.Text.Json;
using Confluent.Kafka;
using CoverPool.Contracts;

const string Topic = "loan-events";
var bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "localhost:9092";

var config = new ProducerConfig
{
    BootstrapServers = bootstrap,

    // Attendre l'accusé de réception de toutes les répliques synchronisées.
    // Sur un broker unique ça ne change rien ; en production c'est la
    // différence entre « envoyé » et « durablement écrit ».
    Acks = Acks.All,

    // Le producteur déduplique ses propres réessais grâce à un identifiant et
    // un numéro de séquence. Sans ça, un timeout réseau suivi d'un réessai
    // écrit le message deux fois.
    EnableIdempotence = true,
};

using var producer = new ProducerBuilder<string, string>(config).Build();

Console.WriteLine($"Producteur connecté à {bootstrap}, topic « {Topic} »\n");

foreach (var evenement in Scenario())
{
    var message = new Message<string, string>
    {
        // LA ligne importante de tout ce fichier.
        // La clé décide de la partition. Même prêt → même partition → ordre
        // garanti pour ce prêt. Sans clé, Kafka répartit au hasard et la
        // réévaluation peut être traitée après le remboursement.
        Key = evenement.LoanId,
        Value = JsonSerializer.Serialize(evenement),
    };

    var resultat = await producer.ProduceAsync(Topic, message);

    Console.WriteLine(
        $"→ {evenement.LoanId,-10} {evenement.Type,-11} " +
        $"partition {resultat.Partition.Value}  offset {resultat.Offset.Value}");

    await Task.Delay(250);
}

// Vide le tampon interne avant de sortir. Sans ce Flush, un producteur qui
// s'arrête juste après ProduceAsync peut perdre ce qui n'est pas encore parti.
producer.Flush(TimeSpan.FromSeconds(10));
Console.WriteLine("\nTerminé.");

static IEnumerable<LoanEvent> Scenario()
{
    var t = DateTimeOffset.UtcNow;

    // ── CH-0001 : la démonstration de l'ordre ────────────────────────────
    // Ces trois événements n'ont de sens que dans cet ordre. Joués à
    // l'envers, le prêt finit éligible alors qu'il ne devrait pas l'être.
    yield return LoanEvent.Originated("CH-0001", 400_000m, 500_000m, t);   // LTV 80,0 % → éligible
    yield return LoanEvent.Revalued("CH-0001", 450_000m, t.AddSeconds(1)); // LTV 88,9 % → sort du pool
    yield return LoanEvent.Repaid("CH-0001", 50_000m, t.AddSeconds(2));    // LTV 77,8 % → revient

    // ── CH-0002 : défaut ─────────────────────────────────────────────────
    yield return LoanEvent.Originated("CH-0002", 300_000m, 600_000m, t);   // LTV 50 % → éligible
    yield return LoanEvent.Defaulted("CH-0002", t.AddSeconds(3));          // sort définitivement

    // ── CH-0003 : refusé dès l'entrée, quotité trop haute ────────────────
    yield return LoanEvent.Originated("CH-0003", 480_000m, 500_000m, t);   // LTV 96 % → jamais éligible

    // ── CH-0004 : devise non conforme ────────────────────────────────────
    yield return LoanEvent.Originated("CH-0004", 200_000m, 400_000m, t, "EUR");

    // ── Du volume, pour voir la répartition entre partitions ─────────────
    var alea = new Random(42);
    for (var i = 5; i <= 14; i++)
    {
        var valeur = alea.Next(400, 1_200) * 1_000m;
        var quotite = alea.Next(45, 95) / 100m;
        yield return LoanEvent.Originated(
            $"CH-{i:D4}", Math.Round(valeur * quotite), valeur, t.AddSeconds(i));
    }
}
