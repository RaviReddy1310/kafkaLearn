package com.learn.kafkademo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ProducerWithCallbacks {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerWithCallbacks.class);

        String bootstrapServer = "localhost:9092";

        //create producer properties
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //create a kafka producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(configs);

        for(int i = 0; i < 5; i++) {

            String topic = "first_topic";
            String value = "First msg sent via java"+Integer.toString(i);
            String key = "key_"+Integer.toString(i);

            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("key is "+key);

            //send message - asynchronous
            kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null) {
                        logger.info("Message Sent Successfully..."+"\n"+
                                "Details of Metadata are:"+"\n"+
                                "Topic: "+metadata.topic()+"\n"+
                                "Partition: "+metadata.partition()+"\n"+
                                "Offset"+metadata.offset());
                    } else {
                        logger.error("Error in sending message", exception);
                    }
                }
            }).get();
        }

        //flushes the data
        kafkaProducer.flush();
        //flushes and closes the producer
        kafkaProducer.close();
    }
}
