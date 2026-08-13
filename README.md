# Cover Pool — pipeline d'événements Kafka en .NET

Un producteur émet des événements de crédit hypothécaire. Un consommateur les
rejoue pour maintenir l'état d'un *cover pool* : quotité de financement,
éligibilité, total du pool.

C'est le domaine de l'offre Luxoft/UBS, monté pour rendre vraie la phrase
« producer/consumer apps in .NET with the Confluent client, topics, partitions,
consumer groups ».

---

# Étape 0 — Kafka, depuis ce que tu connais déjà

Trois idées fausses à évacuer avant de toucher au code.

### Kafka n'est pas une file d'attente

Une file (MSMQ, RabbitMQ), tu y déposes un message, quelqu'un le prend, **il
disparaît**. Kafka ne fait pas ça.

**Un topic Kafka est un journal append-only qui n'est pas vidé par la lecture.**
Le message reste. Dix consommateurs différents peuvent lire le même message.
Tu peux revenir en arrière et tout relire demain.

L'équivalent que tu connais : **le journal des transactions de SQL Server.** Une
suite d'opérations écrites dans l'ordre, conservées, qu'on peut rejouer pour
reconstruire un état. C'est exactement le modèle mental.

### L'offset est un marque-page, pas un pointeur de file

Chaque consommateur note où il en est : c'est l'**offset**. Il est stocké par
**groupe de consommateurs**, pas par message. Deux équipes qui lisent le même
topic ont deux marque-pages indépendants.

Remettre l'offset à zéro = tout relire. C'est une fonctionnalité, pas un
incident.

### Les partitions, c'est le journal découpé en morceaux

Un topic à 3 partitions, c'est 3 fichiers journaux indépendants. Pourquoi ?
Parce qu'un seul fichier ne se lit que séquentiellement — trois se lisent en
parallèle.

**Le prix à payer : l'ordre n'est garanti qu'à l'intérieur d'une partition.**
Jamais globalement sur le topic.

Et c'est là qu'intervient la seule idée vraiment importante de tout ce projet.

---

# Étape 1 — La clé décide de tout

Quand tu produis un message, tu donnes une **clé**. Kafka la hache et en déduit
la partition — exactement comme un `Dictionary<K,V>` choisit son seau.

```
hash("CH-0001") % 3  →  partition 0     ← toujours la même
hash("CH-0006") % 3  →  partition 1
```

**Conséquence : même clé → même partition → ordre garanti pour cette clé.**

Prends CH-0001 dans ce projet :

```
Originated  400 000 sur un bien à 500 000   →  LTV 80,0 %  → éligible
Revalued    le bien retombe à 450 000       →  LTV 88,9 %  → sort du pool
Repaid      50 000 remboursés               →  LTV 77,8 %  → revient
```

Ces trois événements n'ont de sens que dans cet ordre. Joués à l'envers, le prêt
finit éligible alors qu'il ne devrait pas l'être — et un chiffre faux part au
régulateur.

**Sans clé, Kafka répartit au hasard entre les partitions et cet ordre n'est plus
garanti.** C'est la ligne à retenir de tout le projet :

```csharp
Key = evenement.LoanId,
```

---

# Étape 2 — Démarrer le broker

```bash
cd ~/Documents/cover-pool-kafka
./kafka-up.sh
```

Kafka prend environ 1 Go de RAM. **Ferme Chrome avant**, ta machine en a 7,6 au
total.

Le script démarre Kafka en mode **KRaft** — la version moderne, sans Zookeeper.
Si tu tombes sur de la documentation qui parle de Zookeeper, elle est
antérieure à 2022.

Il crée aussi le topic avec **3 partitions**, et affiche :

```
Topic: loan-events   PartitionCount: 3   ReplicationFactor: 1
    Partition: 0    Leader: 1    Replicas: 1    Isr: 1
    Partition: 1    Leader: 1    Replicas: 1    Isr: 1
    Partition: 2    Leader: 1    Replicas: 1    Isr: 1
```

**ReplicationFactor: 1** — une seule copie. En production ce serait 3, réparties
sur trois brokers. **Isr** = *in-sync replicas*, celles qui sont à jour.

---

# Étape 3 — Le producteur

`src/CoverPool.Producer/Program.cs`

Trois réglages, et il faut savoir défendre les trois :

```csharp
Acks = Acks.All,
```
N'accuser réception qu'une fois le message écrit sur **toutes** les répliques
synchronisées. Avec `Acks.Leader`, tu es confirmé dès que le leader l'a — et si
le leader tombe avant réplication, le message est perdu. En banque, `All`.

```csharp
EnableIdempotence = true,
```
Le producteur numérote ses messages. Si le réseau coupe après l'écriture mais
avant l'accusé de réception, le client réessaie — et **sans ce réglage le
message est écrit deux fois**. Avec, le broker reconnaît le numéro de séquence
et l'ignore.

```csharp
producer.Flush(TimeSpan.FromSeconds(10));
```
Le client accumule les messages dans un tampon pour les envoyer par lots. Sortir
sans `Flush` perd ce qui n'est pas encore parti. **Piège classique.**

### Lance-le

```bash
dotnet run --project src/CoverPool.Producer
```

```
→ CH-0001    Originated  partition 0  offset 0
→ CH-0001    Revalued    partition 0  offset 1
→ CH-0001    Repaid      partition 0  offset 2
→ CH-0006    Originated  partition 1  offset 0
→ CH-0008    Originated  partition 2  offset 0
```

**Regarde bien :** les trois CH-0001 sont sur la partition 0. CH-0006 sur la 1,
CH-0008 sur la 2. Les offsets repartent de 0 dans chaque partition — ils sont
locaux à la partition, pas globaux.

---

# Étape 4 — Le consommateur

`src/CoverPool.Consumer/Program.cs`

```csharp
GroupId = "cover-pool-builder",
```
Le **groupe**. Tous les consommateurs qui partagent ce nom se répartissent les
partitions, et **chaque partition n'est lue que par un seul d'entre eux**.
Conséquence directe : ton parallélisme est plafonné par le nombre de partitions.
Dix consommateurs sur trois partitions, sept ne font rien.

```csharp
AutoOffsetReset = AutoOffsetReset.Earliest,
```
Où commencer quand le groupe n'a jamais lu ce topic. `Earliest` = depuis le
début du journal. `Latest` = seulement ce qui arrive après. Ça ne s'applique
**que** la première fois ; ensuite l'offset enregistré fait foi.

```csharp
EnableAutoCommit = false,
...
consumer.Commit(resultat);   // APRÈS traitement
```

Voilà la décision la plus importante du fichier. Tu valides l'offset **après**
avoir traité le message.

- Commit **après** traitement → si tu plantes entre les deux, le message est
  relu au redémarrage. C'est **at-least-once** : tu ne perds rien, tu peux
  doubler.
- Commit **avant** traitement → si tu plantes, le message est perdu
  définitivement. C'est **at-most-once**.

En banque, on ne perd pas un message. Donc at-least-once, **donc des doublons**,
donc :

```csharp
var dejaTraites = new HashSet<string>();
...
if (!dejaTraites.Add(evenement.EventId)) { /* ignoré */ }
```

**L'idempotence.** Et ce n'est pas théorique dans ce projet : `Repaid` porte un
**delta**, pas une valeur absolue.

```csharp
OutstandingPrincipal -= e.RepaymentAmount ?? 0;
```

Rejoué deux fois, le remboursement se soustrait deux fois, le capital restant
est faux, le LTV est faux, l'éligibilité est fausse. **Un doublon devient un
chiffre faux dans un rapport réglementaire.**

C'est l'exemple concret à sortir lundi quand on te parlera d'idempotence.

### Lance-le

Dans un second terminal, pendant que Kafka tourne :

```bash
dotnet run --project src/CoverPool.Consumer
```

```
[C1] partitions attribuées : 0, 1, 2

[C1] p0 o0   CH-0001  Originated  ENTRE LTV 80.0%
        └─ pool : 7 prêts éligibles, 3,022,760 CHF · 1 exclus
[C1] p0 o1   CH-0001  Revalued    SORT  LTV 88.9% > 80%
        └─ pool : 6 prêts éligibles, 2,622,760 CHF · 2 exclus
[C1] p0 o2   CH-0001  Repaid      ENTRE LTV 77.8%
        └─ pool : 7 prêts éligibles, 2,972,760 CHF · 1 exclus
```

Un seul consommateur → il reçoit les trois partitions.

---

# Étape 5 — Les quatre manips qui prouvent que tu as compris

Ce sont elles qu'on te demandera de raconter, pas le code.

## 5.1 — Rejouer le journal depuis zéro

```bash
KAFKA_GROUP=essai-$RANDOM dotnet run --project src/CoverPool.Consumer
```

Nouveau nom de groupe = nouveau marque-page = tout est relu depuis le début, et
l'état du pool se reconstruit à l'identique.

**Ce que ça démontre :** les données ne sont pas consommées, elles sont
conservées. L'état est une **projection** du journal, pas une base de vérité.
Si ta logique d'éligibilité change demain, tu rejoues tout avec les nouvelles
règles.

## 5.2 — Le rééquilibrage

Laisse le premier consommateur tourner. Dans un troisième terminal :

```bash
CONSUMER_NAME=C2 dotnet run --project src/CoverPool.Consumer
```

Regarde les deux terminaux. Tu verras :

```
[C1] partitions retirées : 0, 1, 2
[C1] partitions attribuées : 0, 1
[C2] partitions attribuées : 2
```

**Le rééquilibrage.** Kafka a redistribué. Note que **la consommation s'arrête
pendant l'opération** — c'est pour ça qu'on évite les groupes instables.

Lance un troisième consommateur : une partition chacun. Un quatrième : il reste
inactif. **Le parallélisme est plafonné par le nombre de partitions.**

## 5.3 — Tuer un consommateur

Ctrl+C sur C2. C1 récupère ses partitions en quelques secondes.

Le code appelle `consumer.Close()` dans le `finally` : ça prévient le groupe
immédiatement. Sans ça, il faut attendre l'expiration de la session
(`session.timeout.ms`, 45 secondes par défaut) avant que Kafka comprenne que le
consommateur est parti.

## 5.4 — Les doublons

Relance le producteur pendant que le consommateur tourne. Les `EventId` sont
regénérés, donc ce sont de nouveaux événements — mais tu peux forcer la
démonstration en relançant le consommateur sur un groupe existant après un
`kill -9` : les messages non validés reviennent, et tu verras

```
[C1] doublon ignoré : a3f9c21e0b44 (CH-0001)
```

---

# Étape 6 — Arrêter

```bash
./kafka-down.sh            # arrête, garde le journal
./kafka-down.sh --purge    # supprime tout
```

---

# Ce que tu dis lundi

> I built a loan-event pipeline: a producer emitting origination, revaluation,
> repayment and default events, and a consumer that replays them to maintain a
> cover pool projection — LTV, eligibility, pool total.
>
> The design decision that matters is the message key. Events are keyed by loan
> ID, so all events for one loan land on the same partition and stay ordered.
> A revaluation followed by a repayment gives a different LTV than the reverse,
> so ordering per loan is a correctness requirement, not a nice-to-have.
>
> I commit offsets after processing, which gives at-least-once delivery — so
> duplicates are possible, and the handler has to be idempotent. That's real
> here, not theoretical: the repayment event carries a delta, so replaying it
> would subtract twice and put a wrong figure in the pool. I deduplicate on an
> event ID.

Si on creuse :

- **« Pourquoi 3 partitions ? »** — Le parallélisme des consommateurs est
  plafonné par le nombre de partitions. On les dimensionne sur le débit attendu,
  en sachant qu'augmenter est facile mais que **diminuer ne l'est pas**, et
  qu'ajouter des partitions change le hachage et donc l'affectation des clés
  existantes.
- **« Et l'exactly-once ? »** — Producteur idempotent plus transactions. Ça
  fonctionne à l'intérieur de Kafka. Dès que l'effet de bord sort de Kafka —
  une écriture en base — il faut un outbox ou un traitement idempotent. Je suis
  parti sur l'idempotence, plus simple et plus robuste.
- **« Et si le consommateur prend du retard ? »** — On surveille le *consumer
  lag*, l'écart entre le dernier offset produit et le dernier offset validé.
  C'est la métrique de santé d'un pipeline Kafka.

---

## Structure

```
src/CoverPool.Contracts/    LoanEvent — ce qui circule sur le topic
src/CoverPool.Producer/     émission des événements, clé = LoanId
src/CoverPool.Consumer/     projection du pool, règles d'éligibilité, déduplication
kafka-up.sh / kafka-down.sh Kafka mono-nœud en KRaft, topic à 3 partitions
```

## Ce que ce projet ne fait pas

Pas de persistance — la projection est en mémoire et repart de zéro à chaque
lancement. Pas de schéma (Avro, Schema Registry) : du JSON, donc rien
n'empêche un producteur de casser le contrat. Pas de tests. Pas de gestion des
messages non traitables (*dead letter*). Un seul broker, donc aucune tolérance
aux pannes.

C'est un projet de démonstration monté en un week-end, et c'est ce qu'il faut
dire.
