package com.example.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProducerDemoWithCallback {
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) {
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello world " + i);

            // send data
            producer.send(record, (metadata, e) -> {
                // executes every time a record is successfully sent or an exception is thrown
                if (e != null) {
                    log.error("Error while producing", e);
                } else {
                    log.info("Received new metadata. \n"
                             + "Topic: " + metadata.topic() + "\n"
                             + "Partition: " + metadata.partition() + "\n"
                             + "Offset: " + metadata.offset() + "\n"
                             + "Timestamp: " + metadata.timestamp());
                }
            });
        }

        // flush data
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
