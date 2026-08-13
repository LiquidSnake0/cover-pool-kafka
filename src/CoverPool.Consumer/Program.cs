using System.Text.Json;
using Confluent.Kafka;
using CoverPool.Consumer;
using CoverPool.Contracts;

const string Topic = "loan-events";

var bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "localhost:9092";
var groupe = Environment.GetEnvironmentVariable("KAFKA_GROUP") ?? "cover-pool-builder";
var nom = Environment.GetEnvironmentVariable("CONSUMER_NAME") ?? "C1";

var config = new ConsumerConfig
{
    BootstrapServers = bootstrap,

    // Tous les consommateurs qui partagent ce groupe se répartissent les
    // partitions. Chaque partition n'est lue que par un seul d'entre eux.
    GroupId = groupe,

    // Où commencer quand le groupe n'a jamais lu ce topic. Earliest = depuis
    // le début du journal. Latest = seulement les nouveaux messages.
    AutoOffsetReset = AutoOffsetReset.Earliest,

    // On valide l'offset nous-mêmes, APRÈS traitement. C'est ce qui donne la
    // garantie « au moins une fois » : si le processus tombe entre le
    // traitement et la validation, le message sera relu au redémarrage.
    EnableAutoCommit = false,
};

using var consumer = new ConsumerBuilder<string, string>(config)
    .SetPartitionsAssignedHandler((_, partitions) =>
        Console.WriteLine($"\n[{nom}] partitions attribuées : " +
                          string.Join(", ", partitions.Select(p => p.Partition.Value)) + "\n"))
    .SetPartitionsRevokedHandler((_, partitions) =>
        Console.WriteLine($"\n[{nom}] partitions retirées : " +
                          string.Join(", ", partitions.Select(p => p.Partition.Value)) + "\n"))
    .Build();

consumer.Subscribe(Topic);

// La projection : l'état du pool, dérivé des événements.
var pool = new Dictionary<string, LoanState>();

// La déduplication. Kafka livre « au moins une fois » : après un plantage ou
// un rééquilibrage, tu REVERRAS des messages déjà traités. Sans ce garde-fou,
// un remboursement rejoué se soustrait deux fois.
var dejaTraites = new HashSet<string>();

var arret = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; arret.Cancel(); };

Console.WriteLine($"[{nom}] groupe « {groupe} », en attente sur « {Topic} »");
Console.WriteLine($"[{nom}] Ctrl+C pour arrêter\n");

try
{
    while (!arret.IsCancellationRequested)
    {
        var resultat = consumer.Consume(TimeSpan.FromMilliseconds(500));
        if (resultat is null) continue;

        var evenement = JsonSerializer.Deserialize<LoanEvent>(resultat.Message.Value);
        if (evenement is null) continue;

        if (!dejaTraites.Add(evenement.EventId))
        {
            Console.WriteLine($"[{nom}] doublon ignoré : {evenement.EventId} ({evenement.LoanId})");
            consumer.Commit(resultat);
            continue;
        }

        if (!pool.TryGetValue(evenement.LoanId, out var pret))
            pool[evenement.LoanId] = pret = new LoanState { LoanId = evenement.LoanId };

        var etaitEligible = pret.IsEligible;
        pret.Apply(evenement);

        var (eligible, raison) = EligibilityRules.Evaluate(pret);
        pret.IsEligible = eligible;

        var fleche = (etaitEligible, eligible) switch
        {
            (false, true) => "ENTRE",
            (true, false) => "SORT ",
            _             => "     ",
        };

        Console.WriteLine(
            $"[{nom}] p{resultat.Partition.Value} o{resultat.Offset.Value,-3} " +
            $"{evenement.LoanId,-8} {evenement.Type,-11} {fleche} {raison}");

        // Validation après traitement. Inverser ces deux lignes ferait passer
        // la garantie à « au plus une fois » : un plantage entre les deux
        // perdrait le message définitivement.
        consumer.Commit(resultat);

        if (fleche.Trim().Length > 0) Resumer(pool, nom);
    }
}
catch (OperationCanceledException) { }
finally
{
    // Quitter proprement déclenche un rééquilibrage immédiat au lieu de
    // laisser le groupe attendre l'expiration de la session.
    consumer.Close();
    Console.WriteLine($"\n[{nom}] fermé.");
    Resumer(pool, nom);
}

static void Resumer(Dictionary<string, LoanState> pool, string nom)
{
    var eligibles = pool.Values.Where(p => p.IsEligible).ToList();
    var total = eligibles.Sum(p => p.OutstandingPrincipal);
    var exclus = pool.Count - eligibles.Count;

    Console.WriteLine(
        $"        └─ pool : {eligibles.Count} prêts éligibles, " +
        $"{total:N0} CHF · {exclus} exclus");
}
