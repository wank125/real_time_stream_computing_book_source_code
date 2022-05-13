package com.alain898.book.realtimestreaming.common.kafka;

import com.google.common.base.Preconditions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

/**
 * Created by alain on 15/11/19.
 */
public class KafkaWriter {

    private Properties props;
    private KafkaProducer<String, byte[]> producer;

    final static protected Properties DEFAULT_KAFKA_PROPERTIES = new Properties();

    static {
        // DEFAULT_KAFKA_PROPERTIES.put("metadata.broker.list", "127.0.0.1:9092");
        // DEFAULT_KAFKA_PROPERTIES.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        // 0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        // 1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (must messages that were written to the now-dead leader but not yet replicated will be lost).
        // -1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        // DEFAULT_KAFKA_PROPERTIES.put("request.required.acks", "1");
        // DEFAULT_KAFKA_PROPERTIES.put("partitioner.class", "com.alain898.book.realtimestreaming.common.kafka.WriterPartitioner");
        // DEFAULT_KAFKA_PROPERTIES.put("key.serializer.class", "kafka.serializer.StringEncoder");
        // DEFAULT_KAFKA_PROPERTIES.put("producer.type", "async");
        // DEFAULT_KAFKA_PROPERTIES.put("batch.num.messages", "100")

        DEFAULT_KAFKA_PROPERTIES.put(BOOTSTRAP_SERVERS_CONFIG, "192.168.165.43:9092");
        DEFAULT_KAFKA_PROPERTIES.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
        DEFAULT_KAFKA_PROPERTIES.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        DEFAULT_KAFKA_PROPERTIES.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        DEFAULT_KAFKA_PROPERTIES.put("retries", 3);

        DEFAULT_KAFKA_PROPERTIES.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
    }

    public KafkaWriter(final String kafkaBroker) {
        Preconditions.checkNotNull(kafkaBroker, "kafkaBroker is null");

        Properties kafkaProp = new Properties();
        kafkaProp.put("bootstrap.servers", kafkaBroker);
        this.props = PropertiesUtil.newProperties(DEFAULT_KAFKA_PROPERTIES, kafkaProp);
        this.producer = new KafkaProducer<>(props);
    }

    public KafkaWriter(final Properties props) {
        Preconditions.checkNotNull(props, "props is null");
        this.props = PropertiesUtil.newProperties(DEFAULT_KAFKA_PROPERTIES, props);
        this.producer = new KafkaProducer<String, byte[]>(props);
    }

    public void send(String topic, String key, byte[] message) {
        producer.send(new ProducerRecord<String, byte[]>(topic, key, message));
    }

    public void send(String topic, byte[] message) {
        this.send(topic, null, message);
    }

}

