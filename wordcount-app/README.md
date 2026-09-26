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
bin/kafka-storage.sh format \
    -t $(bin/kafka-storage.sh random-uuid) \
    -c config/kraft/replication-server.properties

# Start the broker
bin/kafka-server-start.sh config/kraft/replication-server.properties
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
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.keys=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
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

## Project structure

```
wordcount-app/
├── pom.xml                                          # Maven build (kafka-streams 4.3.1)
└── src/main/java/com/example/kafka/
    └── WordCountApp.java                            # Streams topology + main()
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
