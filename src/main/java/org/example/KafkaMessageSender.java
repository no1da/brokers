package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class KafkaMessageSender {

    private final KafkaProducer<String, String> producer;

    public KafkaMessageSender(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    public void sendMessage(String topic, String message) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        producer.send(record).get();
    }

    public void close() {
        producer.close();
    }
}