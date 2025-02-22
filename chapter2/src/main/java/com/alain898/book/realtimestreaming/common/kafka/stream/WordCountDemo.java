package com.alain898.book.realtimestreaming.common.kafka.stream;


import com.alain898.book.realtimestreaming.chapter2.datacollector.netty.RequestMsg;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates, using the high-level KStream DSL, how to implement the WordCount program
 * that computes a simple word occurrence histogram from an input text.
 * <p>
 * In this example, the input stream reads from a topic named "streams-plaintext-input", where the values of messages
 * represent lines of text; and the histogram output is written to topic "streams-wordcount-output" where each record
 * is an updated count of a single word.
 * <p>
 * Before running this example you must create the input topic and the output topic (e.g. via
 * {@code bin/kafka-topics.sh --create ...}), and write some data to the input topic (e.g. via
 * {@code bin/kafka-console-producer.sh}). Otherwise you won't see any data arriving in the output topic.
 */
public final class WordCountDemo {
    public static final String topic = "collector_event";

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.165.42:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, byte[]> source = builder.stream(topic);

        KTable<String, Long> counts = source.map(new KeyValueMapper<String, byte[], KeyValue<String, RequestMsg>>() {
                    @Override
                    public KeyValue<String, RequestMsg> apply(String key, byte[] value) {
                        RequestMsg requestMsg = JSONObject.parseObject(new String(value, Charsets.UTF_8), RequestMsg.class);
                        return new KeyValue<>(key, requestMsg);
                    }
                }).map(new KeyValueMapper<String, RequestMsg, KeyValue<String, String>>() {

                    @Override
                    public KeyValue<String, String> apply(String key, RequestMsg value) {
                        return new KeyValue<String, String>(key, value.getIp());
                    }
                }).groupBy((key, value) -> value)
                .count();

        counts.toStream().foreach((key, value) -> {
            System.out.println("key: " + key + ",count: " + value);
        });


        //   .to("wordcount-output", Produced.with(Serdes.String(), Serdes.String()));
        //  counts.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

//        final KTable<String, Long> counts = source
//                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
//                .groupBy((key, value) -> value)
//                .count();
        // need to override value serde to Long type
        //counts.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}

