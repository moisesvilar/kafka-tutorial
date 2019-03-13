package com.moisesvilar.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i=0; i<10; i++) {

            String topic = "first_topic";
            String value = "Hello World! " + i;
            String key = "id_" + i;

            logger.info("Key: " + key);

            // Create the Record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // Send data - synchronous
            producer.send(record, (recordMetadata, e) -> {
                // executes every time record is successfully or exception is true
                if (e == null) {
                    // the record was successfully sent
                    logger.info(
                            "Receive new metadata.\n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset:" + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp()
                    );
                }
                else {
                    logger.error("Error while producing", e);
                }
            }).get(); // block the .send() to make it synchronous - don't do this in production!
        }

        // Flush data
        producer.flush();

        // Close Producer
        producer.close();
    }
}
