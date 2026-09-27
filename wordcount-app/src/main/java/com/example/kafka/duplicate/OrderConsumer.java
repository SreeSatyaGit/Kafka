package com.example.kafka.duplicate;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Demonstrates why naive auto-commit consumers produce duplicates on crash/restart
 * (SAFE_MODE=false) and how manual commit with cooperative rebalance eliminates them
 * (SAFE_MODE=true).
 *
 * Run:
 *   java -cp target/duplicate-demo-1.0-SNAPSHOT.jar \
 *        com.example.kafka.duplicate.OrderConsumer
 */
public class OrderConsumer {

    // -----------------------------------------------------------------------
    // TOGGLE: flip to true, run `mvn -q package`, re-run to see AFTER behavior
    // -----------------------------------------------------------------------
    static final boolean SAFE_MODE = true;

    static final String TOPIC          = "orders";
    static final String BOOTSTRAP      = "localhost:9092";
    static final String GROUP_ID       = "order-consumer-group";
    static final int    PROGRESS_EVERY = 500; // print stats every N records

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Start from the earliest available offset so test runs always see all events
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");

        if (!SAFE_MODE) {
            // ----------------------------------------------------------------
            // BEFORE — naive consumer with auto-commit
            //
            // enable.auto.commit=true: Kafka automatically commits the highest
            //   offset returned by the most recent poll() call, on the NEXT
            //   poll() call, if the commit interval has elapsed.
            //
            // auto.commit.interval.ms=5000: the committed offset advances
            //   every ~5 seconds.  The committed position reflects the last
            //   BATCH BOUNDARY, not "how far we actually finished processing."
            //
            // WHY THIS CAUSES DUPLICATES:
            //   1. poll() returns records at offsets [0..99].
            //   2. The app processes records 0..49 (each takes 50 ms → 2.5 s).
            //   3. The process is killed (Ctrl+C) at offset 49.
            //   4. The auto-commit interval hasn't fired yet (< 5 s elapsed),
            //      so the last committed offset is still wherever it was before
            //      this batch (e.g. -1 / beginning).
            //   5. On restart the consumer resumes from the committed offset.
            //      → offsets [0..49] are redelivered and processed AGAIN.
            //      → seenCounts for those eventIds goes from 1 to 2 — duplicates.
            //
            // A SUBTLER VARIANT (silent data loss):
            //   If the process is slow but not killed, and the auto-commit fires
            //   after 5 s while we're still processing offset 50, the committed
            //   position advances to 100.  If the process then crashes, offsets
            //   [50..99] were committed but never fully processed — silently lost.
            // ----------------------------------------------------------------
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,  "5000");
            System.out.println("[MODE] SAFE_MODE=false — auto-commit, 50 ms sleep/record (easy dup window)");

        } else {
            // ----------------------------------------------------------------
            // AFTER — safe consumer: manual commit + cooperative rebalance
            //
            // enable.auto.commit=false: the application controls exactly when
            //   offsets are committed — only after every record in a batch is
            //   fully processed (see commitSync() below).  A crash before
            //   commitSync() replays at most one uncommitted batch, which is
            //   bounded by max.poll.records (500 records here).
            //
            // isolation.level=read_committed: the consumer skips messages that
            //   belong to open (not-yet-committed) transactions.  Even without a
            //   transactional producer in this demo this is a defensive best
            //   practice — it prevents consuming "tentative" writes that may be
            //   aborted, sparing the app from compensating/rollback logic.
            //
            // partition.assignment.strategy=CooperativeStickyAssignor:
            //   The default RangeAssignor does a full "stop-the-world" rebalance:
            //   ALL consumers drop ALL their partitions, then reacquire them.
            //   During the blackout window no consumer polls, which can exceed
            //   max.poll.interval.ms and trigger ANOTHER rebalance — plus every
            //   consumer replays everything since its last commit, causing a burst
            //   of duplicates.  CooperativeStickyAssignor only migrates the
            //   partitions that actually need to move; unaffected partitions keep
            //   processing with no pause and no redelivery.
            //
            // session.timeout.ms=10000: if the broker receives no heartbeat for
            //   10 s it declares the consumer dead and triggers a rebalance.
            //   Shorter = faster detection of dead consumers (less duplicate
            //   replay on restart) but higher risk of false-positive rebalances
            //   caused by GC pauses or momentary network jitter.
            //
            // heartbeat.interval.ms=3000: heartbeats are sent every 3 s.  Must
            //   be < session.timeout.ms / 3 to guarantee at least 3 heartbeats
            //   reach the broker before the session expires.  3000 < 10000/3=3333.
            //
            // max.poll.interval.ms=300000 (5 min): if poll() is not called
            //   within 5 minutes the broker assumes the consumer is hung and
            //   kicks it out, triggering a rebalance.  Set this above the slowest
            //   possible batch-processing time; if it fires mid-batch the whole
            //   batch is revoked and will be redelivered.
            //
            // max.poll.records=500: caps how many records one poll() returns.
            //   Bounds the worst-case redelivery burst on crash-before-commit to
            //   ≤ 500 records instead of potentially thousands.
            // ----------------------------------------------------------------
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,             "false");
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,                "read_committed");
            props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                      CooperativeStickyAssignor.class.getName());
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,             "10000");
            props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,          "3000");
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,           "300000");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,               "500");
            System.out.println("[MODE] SAFE_MODE=true — manual commit, cooperative rebalance, tuned timeouts");
        }

        // In-memory duplicate tracker — key = eventId, value = delivery count
        Map<String, Integer> seenCounts = new HashMap<>();
        int[] totalProcessed     = {0}; // array for mutation inside lambda
        int[] duplicateDeliveries = {0}; // excess deliveries = totalProcessed - unique count

        AtomicBoolean running = new AtomicBoolean(true);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            // Shutdown hook: signal the poll loop to exit cleanly on Ctrl+C
            Runtime.getRuntime().addShutdownHook(new Thread("consumer-shutdown") {
                @Override
                public void run() {
                    running.set(false);
                    consumer.wakeup(); // interrupts any blocking poll() call immediately
                }
            });

            consumer.subscribe(Collections.singletonList(TOPIC));
            System.out.printf("Subscribed to '%s' as group '%s'. Waiting for records…%n",
                    TOPIC, GROUP_ID);

            try {
                while (running.get()) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(500));

                    if (records.isEmpty()) continue;

                    for (ConsumerRecord<String, String> record : records) {

                        if (!SAFE_MODE) {
                            // ----------------------------------------------------
                            // Simulate slow per-record processing (e.g. a DB write
                            // that takes 50 ms).  This creates a wide window where:
                            //
                            // (a) Killing the process (Ctrl+C) before the next
                            //     poll() triggers auto-commit will redeliver
                            //     everything in the current batch on restart.
                            //
                            // (b) If slow enough that auto-commit fires mid-batch,
                            //     records processed AFTER the commit checkpoint
                            //     but BEFORE the next commit will be silently lost
                            //     on a crash.
                            //
                            // → Kill and restart the consumer while it is sleeping
                            //   to reliably trigger duplicates you can observe.
                            // ----------------------------------------------------
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }

                        // Parse and track delivery count for this eventId
                        OrderEvent event = JsonUtil.fromJson(record.value());
                        int deliveryCount = seenCounts.merge(event.getEventId(), 1, Integer::sum);
                        totalProcessed[0]++;

                        if (deliveryCount > 1) {
                            // The same eventId has been delivered more than once
                            duplicateDeliveries[0]++;
                            System.out.printf(
                                    "*** DUPLICATE DETECTED ***  eventId=%s  seenCount=%d" +
                                    "  partition=%d  offset=%d%n",
                                    event.getEventId(), deliveryCount,
                                    record.partition(), record.offset());
                        }

                        if (totalProcessed[0] % PROGRESS_EVERY == 0) {
                            printStats(totalProcessed[0], seenCounts.size(),
                                       duplicateDeliveries[0]);
                        }
                    }

                    if (SAFE_MODE) {
                        // --------------------------------------------------------
                        // AFTER: commit only AFTER the entire batch is processed.
                        //
                        // commitSync() blocks until the broker confirms the commit,
                        // so we know exactly where the consumer will resume on
                        // restart.  A crash between the record loop and this line
                        // replays at most one batch (≤ max.poll.records = 500),
                        // not an unbounded window like auto-commit can produce.
                        // --------------------------------------------------------
                        consumer.commitSync();
                    }
                }

            } catch (WakeupException e) {
                // WakeupException is the normal signal from consumer.wakeup() in
                // the shutdown hook — only rethrow if it fired unexpectedly
                if (running.get()) throw e;
            }

        } // consumer.close() called here by try-with-resources

        // Final stats printed after clean shutdown
        System.out.println("\n--- SHUTDOWN SUMMARY ---");
        printStats(totalProcessed[0], seenCounts.size(), duplicateDeliveries[0]);
    }

    private static void printStats(int total, int unique, int dups) {
        double dupRate = total == 0 ? 0.0 : (dups * 100.0 / total);
        System.out.printf(
                "[STATS]  processed=%-6d  unique=%-6d  duplicates=%-6d  dupRate=%.4f%%%n",
                total, unique, dups, dupRate);
    }
}
