package com.learn.kafkademo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Producer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(Producer.class);

        String bootstrapServer = "localhost:9092";
        String topic = "first_topic";
        String value = "First msg sent via java";

        //create producer properties
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //create a kafka producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(configs);

        //create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);

        //send message - asynchronous
        kafkaProducer.send(record);

        //flushes the data
        kafkaProducer.flush();
        //flushes and closes the producer
        kafkaProducer.close();
    }
}
