package com.moisesvilar.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++) {
            // Create the Record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", "Hello World! " + i);

            // Send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
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
                }
            });
        }

        // Flush data
        producer.flush();

        // Close Producer
        producer.close();
    }
}
