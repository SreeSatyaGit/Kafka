package com.example.kafka.duplicate;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Demonstrates how duplicate events are produced (IDEMPOTENT_MODE=false) and
 * how to eliminate them (IDEMPOTENT_MODE=true).
 *
 * Toggle the flag below, rebuild with `mvn -q package`, and compare the
 * duplicate rate printed by OrderConsumer.
 *
 * Run:
 *   java -cp target/duplicate-demo-1.0-SNAPSHOT.jar \
 *        com.example.kafka.duplicate.OrderProducer [numEvents]
 */
public class OrderProducer {

    // -----------------------------------------------------------------------
    // TOGGLE: flip to true, run `mvn -q package`, re-run to see AFTER behavior
    // -----------------------------------------------------------------------
    static final boolean IDEMPOTENT_MODE = true;

    static final String TOPIC             = "orders";
    static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws Exception {
        int totalEvents = args.length > 0 ? Integer.parseInt(args[0]) : 2000;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,        BOOTSTRAP_SERVERS);
        // Both modes serialize keys and values as plain strings (values are JSON)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,     StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());

        if (!IDEMPOTENT_MODE) {
            // ----------------------------------------------------------------
            // BEFORE — naive "at-least-once" producer configuration
            //
            // acks=1: only the partition leader acknowledges the write.  This is
            //   fast, but if the leader fails before replicating the record, the
            //   producer will time out and retry — creating a duplicate on the
            //   new leader (which never saw the original).
            //
            // enable.idempotence=false: the broker assigns no
            //   (ProducerEpoch, SequenceNumber) pair to batches.  Every send()
            //   call is treated as an independent new message.  When Kafka's
            //   internal retry fires (e.g. on a leader election), the broker
            //   cannot tell it is a retry and accepts it as a fresh record.
            //
            // retries=3: Kafka retries up to 3 times on transient network/leader
            //   errors.  Without idempotence each retry can land a duplicate.
            //
            // max.in.flight.requests.per.connection=5: up to 5 batches can be
            //   in-flight simultaneously.  Combined with retries and no
            //   idempotence, this can also cause out-of-order delivery: if batch
            //   #1 fails and is retried while batches #2–5 already succeeded,
            //   the retry of #1 appends AFTER #2–5.
            // ----------------------------------------------------------------
            props.put(ProducerConfig.ACKS_CONFIG,                                "1");
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,                  "false");
            props.put(ProducerConfig.RETRIES_CONFIG,                             "3");
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,      "5");
            System.out.println("[MODE] IDEMPOTENT_MODE=false — naive producer, ~5% of events sent twice");

        } else {
            // ----------------------------------------------------------------
            // AFTER — idempotent "exactly-once per partition" producer
            //
            // enable.idempotence=true: the broker assigns a unique
            //   (ProducerEpoch, SequenceNumber) pair to every batch, per
            //   partition.  If the producer retries a batch that already
            //   landed, the broker recognises the duplicate sequence number and
            //   silently discards it — the record appears exactly once.
            //
            // acks=all: required by enable.idempotence.  All in-sync replicas
            //   must acknowledge before the send completes, so the record
            //   survives a leader failover without needing a client retry.
            //
            // max.in.flight.requests.per.connection=5: maximum allowed with
            //   idempotence.  Kafka tracks up to 5 in-flight sequence numbers
            //   per partition for deduplication; higher values are rejected.
            //
            // KEY = orderId: routing all events for the same order to the same
            //   partition is the prerequisite for broker-side idempotent dedup.
            //   Without a stable key, a retried record could land on a different
            //   partition where the broker has no prior sequence to compare
            //   against — it would accept the record as new, defeating
            //   idempotence.
            // ----------------------------------------------------------------
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,             "true");
            props.put(ProducerConfig.ACKS_CONFIG,                           "all");
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
            System.out.println("[MODE] IDEMPOTENT_MODE=true — idempotent producer, broker deduplicates retries");
        }

        System.out.printf("Sending %d events to topic '%s' (%s)%n",
                totalEvents, TOPIC, BOOTSTRAP_SERVERS);

        // Collect every send() Future so we can block until all broker acks
        // arrive before computing throughput.  This measures actual end-to-end
        // send latency, not just client-side enqueue time.
        List<Future<RecordMetadata>> futures = new ArrayList<>(totalEvents + 200);
        int totalSendCalls = 0; // >= totalEvents when simulated retries fire

        long wallClockStart = System.currentTimeMillis();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            for (int i = 0; i < totalEvents; i++) {
                OrderEvent event = new OrderEvent(
                        UUID.randomUUID().toString(),  // unique eventId per logical event
                        "ORD-" + (i % 500),            // 500 distinct orders cycling
                        10.0 + (i % 100),              // amount
                        System.currentTimeMillis()     // timestamp
                );
                String json = JsonUtil.toJson(event);

                // IDEMPOTENT_MODE: key = orderId so same-order events always land
                // on the same partition — prerequisite for broker-level dedup.
                // Naive mode: key = null → round-robin, retries can hit any partition.
                String key = IDEMPOTENT_MODE ? event.getOrderId() : null;

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, json);

                futures.add(send(producer, record, event.getEventId()));
                totalSendCalls++;

                // ----------------------------------------------------------------
                // BEFORE only — simulate an application-level "retry-after-timeout"
                // bug.
                //
                // Real-world scenario: the app has its own callback timeout (e.g.
                // 200 ms).  If the Kafka ack takes longer, the app assumes failure
                // and calls send() AGAIN for the exact same OrderEvent object.
                //
                // With IDEMPOTENT_MODE=false the broker sees two independent
                // messages (no sequence number links them), accepts both, and the
                // consumer receives the same eventId twice — a duplicate.
                //
                // With IDEMPOTENT_MODE=true this block is removed entirely: the
                // built-in idempotent retry mechanism handles transient errors via
                // sequence numbers, so no application-level retry logic is needed
                // or safe.
                // ----------------------------------------------------------------
                if (!IDEMPOTENT_MODE && ThreadLocalRandom.current().nextInt(100) < 5) {
                    System.out.printf("SIMULATED RETRY — resending eventId=%s%n", event.getEventId());
                    futures.add(send(producer, record, event.getEventId()));
                    totalSendCalls++;
                }

                if ((i + 1) % 500 == 0) {
                    System.out.printf("  Progress: %d / %d events queued  (send() calls so far: %d)%n",
                            i + 1, totalEvents, totalSendCalls);
                }
            }

            // flush() pushes all buffered batches to the broker before we start
            // waiting on futures — avoids a scenario where .get() hangs because
            // a small final batch is still sitting in the producer's internal buffer
            producer.flush();

            // Wait for every broker ack (or error) — this is what makes the
            // measured wall-clock time reflect real end-to-end latency
            for (Future<RecordMetadata> f : futures) {
                f.get();
            }
        }

        long elapsed = System.currentTimeMillis() - wallClockStart;
        double throughput = totalSendCalls / (elapsed / 1000.0);

        System.out.printf("%nDone.%n");
        System.out.printf("  events requested   : %d%n",    totalEvents);
        System.out.printf("  total send() calls : %d   (includes simulated retries in BEFORE mode)%n",
                totalSendCalls);
        System.out.printf("  wall-clock time    : %d ms%n", elapsed);
        System.out.printf("  throughput         : %.1f events/sec%n", throughput);
    }

    /**
     * Wraps producer.send() with an error-logging callback.
     * Returns the Future so the caller can wait for the broker ack.
     */
    private static Future<RecordMetadata> send(
            KafkaProducer<String, String> producer,
            ProducerRecord<String, String> record,
            String eventId) {
        return producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.printf("SEND FAILED  eventId=%s  error=%s%n",
                        eventId, exception.getMessage());
            }
        });
    }
}
