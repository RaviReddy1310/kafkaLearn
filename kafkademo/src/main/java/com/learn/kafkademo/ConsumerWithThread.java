package com.learn.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThread {

    public static void main(String[] args) {

        String bootstrapServer = "localhost:9092";
        String groupId = "first-consumer-group";
        String topic = "first_topic";

        new ConsumerWithThread().run(bootstrapServer, groupId, topic);

    }

    private void run(String bootstrapServer, String groupId, String topic) {

        Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class);

        //latch deals with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //creating a runnable
        Runnable consumerRunnable = new ConsumerRunnable(bootstrapServer, groupId, topic, latch);

        //creating the consumer thread
        Thread consumerThread = new Thread(consumerRunnable);

        //starting the thread
        consumerThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                logger.info("Application is closed");
            }
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application got interrupted");
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {

            //create consumer properties
            Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //create consumer
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(config);

            //subscribe to the topic
            kafkaConsumer.subscribe(Arrays.asList(topic));

            this.kafkaConsumer = kafkaConsumer;
            this.latch = latch;
        }

        @Override
        public void run() {

            Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class);

            try {
                while(true) {
                    //receive msgs from producer
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String, String> record:records)
                        logger.info("Key: "+record.key()+", Value: "+record.value()+"\n"+
                                "Topic: "+record.topic()+", Partition: "+record.partition()+"\n");
                }
            } catch(WakeupException e) {
                logger.info("Received Shutdown signal");
            } finally {
                kafkaConsumer.close();
                //tells our main code that we are done with consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            kafkaConsumer.wakeup();
        }
    }
}
