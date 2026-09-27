package com.example.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams WordCount application – Step 7 of the Apache Kafka Quickstart.
 *
 * Reads text lines from the "quickstart-events" topic, tokenises them into
 * lowercase words, counts occurrences per word, and writes the running word
 * counts to the "output-topic" topic.
 *
 * Run:
 *   java -jar target/wordcount-app-1.0-SNAPSHOT.jar
 *
 * Inspect output:
 *   bin/kafka-console-consumer.sh \
 *       --bootstrap-server localhost:9092 \
 *       --topic output-topic \
 *       --from-beginning \
 *       --formatter kafka.tools.DefaultMessageFormatter \
 *       --property print.keys=true \
 *       --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
 *       --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 */
public class WordCountApp {

    public static void main(String[] args) {

        // ------------------------------------------------------------------ //
        // 1. Streams configuration                                            //
        // ------------------------------------------------------------------ //
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "wordcount-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                  Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                  Serdes.String().getClass());
        // Disable the state-store record cache so every processed record is
        // forwarded immediately to the output topic.
        // NOTE: CACHE_MAX_BYTES_BUFFERING_CONFIG was deprecated in Kafka 3.4;
        //       STATESTORE_CACHE_MAX_BYTES_CONFIG is the current replacement.
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        // ------------------------------------------------------------------ //
        // 2. Topology                                                         //
        // ------------------------------------------------------------------ //
        final StreamsBuilder builder = new StreamsBuilder();

        // Source: read text lines from the input topic
        KStream<String, String> textLines = builder.stream("quickstart-events");

        // Transform: split each line into lowercase words, group, and count
        KTable<String, Long> wordCounts = textLines
                .flatMapValues(line ->
                        Arrays.asList(line.toLowerCase().split("\\W+")))
                .groupBy((keyIgnored, word) -> word)
                .count();

        // Sink: write word counts to the output topic
        wordCounts.toStream()
                  .to("output-topic",
                      Produced.with(Serdes.String(), Serdes.Long()));

        // ------------------------------------------------------------------ //
        // 3. Start the streams application                                    //
        // ------------------------------------------------------------------ //
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Latch that keeps main() alive until a SIGTERM / Ctrl-C is received
        final CountDownLatch latch = new CountDownLatch(1);

        // Graceful shutdown hook – closes streams cleanly on JVM exit
        Runtime.getRuntime().addShutdownHook(new Thread("wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            System.out.println("WordCount application started. Waiting for events on 'quickstart-events'…");
            latch.await();          // block until the shutdown hook fires
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("WordCount application stopped.");
    }
}
