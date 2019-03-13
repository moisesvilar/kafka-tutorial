package com.moisesvilar.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads() {}

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";
        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        // start the consumer runnable
        logger.info("Creating the consumer thread");
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(topic, bootstrapServers, groupId, latch);
        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();
        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        ConsumerRunnable(String topic, String bootstrapServers, String groupId, CountDownLatch latch) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer = new KafkaConsumer<>(properties);
            this.consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                //noinspection InfiniteLoopStatement
                while (true) {
                    ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        this.logger.info("Key: " + record.key());
                        this.logger.info("Value: " + record.value());
                        this.logger.info("Partition: " + record.partition());
                        this.logger.info("Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                this.logger.info("Receive shutdown signal!");
            } finally {
                this.consumer.close();
                // tell our main code we are done with the consumer
                this.latch.countDown();
            }
        }

        void shutdown() {
            // the wakeup method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            this.consumer.wakeup();
        }
    }
}
