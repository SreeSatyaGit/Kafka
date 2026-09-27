# Kafka Streams WordCount — Step 7 of the Apache Kafka Quickstart

A minimal [Kafka Streams](https://kafka.apache.org/documentation/streams/) application that reads text lines from the `quickstart-events` topic, splits them into words, and continuously emits running word counts to the `output-topic` topic.

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Java (JDK)  | 11 +    |
| Maven       | 3.6 +   |
| Apache Kafka | 4.3.1  |

> **Kafka must already be running** with the required topics created before you start the app.

---

## 1. Start Kafka (KRaft mode)

```bash
cd ~/Kafka/kafka_2.13-4.3.1

# Generate a cluster ID (first time only)
# --standalone is required in Kafka 4.x for a single combined broker+controller node
bin/kafka-storage.sh format \
    --standalone \
    -t $(bin/kafka-storage.sh random-uuid) \
    -c config/server.properties

# Start the broker
bin/kafka-server-start.sh config/server.properties
```

---

## 2. Create the required topics

Open a **new terminal** in the Kafka directory:

```bash
cd ~/Kafka/kafka_2.13-4.3.1

# Input topic
bin/kafka-topics.sh --create \
    --topic quickstart-events \
    --bootstrap-server localhost:9092

# Output topic
bin/kafka-topics.sh --create \
    --topic output-topic \
    --bootstrap-server localhost:9092
```

---

## 3. Build the application

```bash
cd ~/Kafka/wordcount-app

/opt/homebrew/bin/mvn package -q
```

This produces a self-contained uber-jar at:

```
target/wordcount-app-1.0-SNAPSHOT.jar
```

---

## 4. Run the WordCount application

```bash
java -jar ~/Kafka/wordcount-app/target/wordcount-app-1.0-SNAPSHOT.jar
```

You should see:

```
WordCount application started. Waiting for events on 'quickstart-events'…
```

Leave this terminal running.

---

## 5. Produce some events

Open a **new terminal**:

```bash
cd ~/Kafka/kafka_2.13-4.3.1

bin/kafka-console-producer.sh \
    --topic quickstart-events \
    --bootstrap-server localhost:9092
```

Type a few lines and press **Enter** after each:

```
hello world
hello kafka streams
this is a wordcount demo
```

Press **Ctrl-C** to exit the producer.

---

## 6. Inspect the word counts

Open yet another terminal to consume the output topic:

```bash
cd ~/Kafka/kafka_2.13-4.3.1

bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic output-topic \
    --from-beginning \
    --formatter-property print.key=true \
    --formatter-property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --formatter-property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

Expected output (word → count):

```
hello   1
world   1
hello   2
kafka   1
streams 1
...
```

---

## 7. Stop the application

Press **Ctrl-C** in the WordCount terminal. The shutdown hook closes Kafka Streams cleanly.

---

## 8. Clean up — delete topics and broker data

### Delete individual topics

```bash
cd ~/Kafka/kafka_2.13-4.3.1

bin/kafka-topics.sh --delete --topic quickstart-events --bootstrap-server localhost:9092
bin/kafka-topics.sh --delete --topic output-topic      --bootstrap-server localhost:9092
```

Verify they are gone:

```bash
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Stop the broker and wipe all data

Stop the broker first (Ctrl-C in its terminal), then delete the log directory and the
`__cluster_metadata` internal topic data that Kafka writes there:

```bash
rm -rf /tmp/kraft-combined-logs
```

This removes everything the broker stored: all topic data, consumer group offsets, and
the KRaft metadata log. The next `kafka-storage.sh format --standalone` will start fresh.

---

## Project structure

```
wordcount-app/
├── pom.xml                                          # Maven build (kafka-streams 4.3.1 + jackson-databind)
└── src/main/java/com/example/kafka/
    ├── WordCountApp.java                            # Streams topology + main()
    └── duplicate/
        ├── OrderEvent.java                          # POJO: eventId, orderId, amount, timestamp
        ├── JsonUtil.java                            # Jackson helper
        ├── OrderProducer.java                       # Demonstrates duplicate production & idempotent fix
        └── OrderConsumer.java                       # Demonstrates auto-commit duplicates & manual-commit fix
```

## Key configuration (WordCountApp.java)

| Property | Value |
|----------|-------|
| `application.id` | `wordcount-application` |
| `bootstrap.servers` | `localhost:9092` |
| Input topic | `quickstart-events` |
| Output topic | `output-topic` |
| Key Serde | `Serdes.String()` |
| Value Serde (output) | `Serdes.Long()` |

---

# Duplicate Event Delivery Demo

A hands-on demo showing **why duplicates happen** in a Kafka producer/consumer pipeline
and how to eliminate them — plain `kafka-clients` API, no extra infrastructure.

## Problem statement

Three independent sources of duplicate delivery exist in a typical Kafka pipeline:

1. **Producer retries without idempotence.** When the producer retries a send (due to a
   network hiccup or leader election), the broker has no way to tell it is the same record
   arriving twice — it appends both.

2. **Consumer auto-commit racing ahead of processing.** With `enable.auto.commit=true`,
   Kafka commits the batch-boundary offset on the next `poll()` call, not when the app
   finishes processing. If the process is killed mid-batch, the entire batch is redelivered
   on restart.

3. **Consumer-group rebalances redelivering uncommitted work.** The default RangeAssignor
   drops all partitions from all consumers simultaneously. Every consumer replays from its
   last committed offset, causing a burst of duplicates on every scale-out or restart.

Toggle `IDEMPOTENT_MODE` in `OrderProducer.java` and `SAFE_MODE` in `OrderConsumer.java`,
rebuild, and compare the duplicate rate the consumer prints.

Both classes are run via `-cp` (not `-jar`) since the jar's default main is `WordCountApp`:

```bash
# Producer
java -cp target/wordcount-app-1.0-SNAPSHOT.jar \
     com.example.kafka.duplicate.OrderProducer [numEvents]

# Consumer
java -cp target/wordcount-app-1.0-SNAPSHOT.jar \
     com.example.kafka.duplicate.OrderConsumer
```

---

## Part 1 — Reproduce the "BEFORE" state (duplicates visible)

### Step 1 — Create the `orders` topic with 3 partitions

```bash
cd ~/Kafka/kafka_2.13-4.3.1

bin/kafka-topics.sh \
    --create --topic orders \
    --partitions 3 --replication-factor 1 \
    --bootstrap-server localhost:9092
```

### Step 2 — Confirm both flags are `false`, then build

In `OrderProducer.java`: `static final boolean IDEMPOTENT_MODE = false;`
In `OrderConsumer.java`: `static final boolean SAFE_MODE = false;`

```bash
cd ~/Kafka/wordcount-app
mvn -q package
```

### Step 3 — Start the consumer (Terminal 1)

```bash
java -cp target/wordcount-app-1.0-SNAPSHOT.jar \
     com.example.kafka.duplicate.OrderConsumer
```

### Step 4 — Start the producer (Terminal 2)

```bash
java -cp target/wordcount-app-1.0-SNAPSHOT.jar \
     com.example.kafka.duplicate.OrderProducer
```

You will see `SIMULATED RETRY — resending eventId=...` lines (~5% of events).

### Step 5 — Kill and restart the consumer to force redelivery

While the producer is running, press **Ctrl+C** in Terminal 1, wait 2–3 seconds, then
restart the consumer with the same command. Repeat once or twice.

### What to look for

```
*** DUPLICATE DETECTED ***  eventId=3f2e1d...  seenCount=2  partition=1  offset=47
[STATS]  processed=500    unique=483    duplicates=17    dupRate=3.4000%
```

The duplicate rate will typically be **2–6%** depending on how many kill/restarts you do.

---

## Part 2 — Fix both sides (near-zero duplicates)

### Step 1 — Flip both flags to `true`

In `OrderProducer.java`: `static final boolean IDEMPOTENT_MODE = true;`
In `OrderConsumer.java`: `static final boolean SAFE_MODE = true;`

### Step 2 — Rebuild

```bash
mvn -q package
```

### Step 3 — Reset the consumer group offset

```bash
cd ~/Kafka/kafka_2.13-4.3.1
bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --delete --group order-consumer-group
```

### Step 4 — Re-run the same test

Start consumer (Terminal 1), start producer (Terminal 2), kill/restart consumer as before.

### Expected result

```
[STATS]  processed=2000   unique=2000   duplicates=0      dupRate=0.0000%
```

No simulated retries from the producer. No redelivery on kill/restart.

---

## Part 3 — Partition & throughput tuning

### Baseline (3 partitions, 1 consumer, 20 000 events)

```bash
# Recreate topic with 3 partitions
cd ~/Kafka/kafka_2.13-4.3.1
bin/kafka-topics.sh --delete --topic orders --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic orders --partitions 3 --replication-factor 1 \
    --bootstrap-server localhost:9092
```

Start 1 consumer (SAFE_MODE=true), then:

```bash
cd ~/Kafka/wordcount-app
java -cp target/wordcount-app-1.0-SNAPSHOT.jar \
     com.example.kafka.duplicate.OrderProducer 20000
```

Note the `throughput` line in the producer output.

### Scaled run (6 partitions, 3 consumers, 20 000 events)

```bash
cd ~/Kafka/kafka_2.13-4.3.1
bin/kafka-topics.sh --delete --topic orders --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic orders --partitions 6 --replication-factor 1 \
    --bootstrap-server localhost:9092
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --delete --group order-consumer-group
```

Start **3 consumer instances** in separate terminals, wait for the group to stabilise,
then run the producer with 20 000 events. Compare throughput numbers.

> **Note:** These are real, machine-specific numbers — not benchmark claims.
> Paste your own results here:
>
> | Config                      | Throughput (your machine) |
> |-----------------------------|---------------------------|
> | 3 partitions, 1 consumer    | _____ events/sec          |
> | 6 partitions, 3 consumers   | _____ events/sec          |

---

## Config change summary

### Producer (BEFORE → AFTER)

| Config key | BEFORE | AFTER | Why |
|---|---|---|---|
| `enable.idempotence` | `false` | `true` | Broker deduplicates retransmitted batches via sequence numbers |
| `acks` | `1` | `all` | Required by idempotence; all ISR members ack, record survives failover |
| `retries` | `3` | default (INT_MAX) | Unlimited retries are safe with idempotence |
| `max.in.flight.requests.per.connection` | `5` | `5` | Max allowed with idempotence; kept explicit for clarity |
| Message key | `null` | `orderId` | Stable key routes same-order events to same partition — prerequisite for broker-side dedup |
| Simulated app retry | ~5% of sends | removed | Idempotence makes the retry-on-timeout pattern safe; no manual retry needed |

### Consumer (BEFORE → AFTER)

| Config key | BEFORE | AFTER | Why |
|---|---|---|---|
| `enable.auto.commit` | `true` | `false` | App commits only after full batch processing — no auto-advance race |
| `auto.commit.interval.ms` | `5000` | removed | Not applicable with manual commit |
| `isolation.level` | `read_uncommitted` | `read_committed` | Skip tentative writes from open transactions |
| `partition.assignment.strategy` | `RangeAssignor` | `CooperativeStickyAssignor` | Incremental rebalance; unaffected partitions keep processing |
| `session.timeout.ms` | `45000` | `10000` | Faster dead-consumer detection = shorter replay window on restart |
| `heartbeat.interval.ms` | `3000` | `3000` | Kept; must stay < session.timeout.ms / 3 |
| `max.poll.interval.ms` | `300000` | `300000` | Kept; set above slowest batch-processing time |
| `max.poll.records` | `500` | `500` | Explicit; caps worst-case redelivery burst on crash-before-commit |
| Processing delay | 50 ms/record | none | Slow processing removed; manual commit eliminates the window |
