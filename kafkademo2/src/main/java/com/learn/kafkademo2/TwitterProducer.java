package com.learn.kafkademo2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private String consumerKey = "3xRB7bUJNVb6e6ACmhrD0vYiS";
    private String consumerSecret = "J08yt4xG0Slx3baXenJjTYYO2fX189CnDuzVeQvZxtF2kteG61";
    private String token = "2884150060-3fRJRA262fzevz3jqhaLpXyo1NtzC6DrPBNAaQx";
    private String tokenSecret = "l9HWjkOJ18zOppkFAmxJaLzbA4akP8iNBVJ9Ivah9X5zq";

    public static void main(String[] args) {

        new TwitterProducer().run();
    }

    private void run() {

        // topic name
        String topic = "twitter_topic";

        // queue that holds the messages
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(10);

        // creating the kafkaProducer
        KafkaProducer<String, String> kafkaProducer = createProducer();
        //creating twitterClient
        Client hosebirdClient = twitterClient(msgQueue);
        hosebirdClient.connect();

        int count = 0;

        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone() && count < 5) {

            String msg = null;
            try {
                msg = msgQueue.take();
            } catch (InterruptedException e) {
                logger.error("HoseBird Client got interrupted", e);
                hosebirdClient.stop();
            }

            if (msg != null) {

                logger.info(msg);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);

                kafkaProducer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception == null) {
                            logger.info("Details of the record sent are:\n"+
                                        "Topic: "+metadata.topic()+
                                        ", Partition: "+metadata.partition()+
                                        ", Offset: "+metadata.offset()+"\n");
                        } else {
                            logger.error("Error occured in sending the msg", exception);
                        }
                    }
                });
            }

            count++;
        }

        logger.info("Reading the msgs is done");
    }

    private Client twitterClient(BlockingQueue<String> msgQueue) {

        // declaring the connection info

        // host to connect to
        Hosts hoseBirdHosts = new HttpHosts(Constants.STREAM_HOST);

        // declare the endpoints of the tweets
        StatusesFilterEndpoint hoseBirdEndpoint = new StatusesFilterEndpoint();
        // Optional: includes locations, followings
        List<String> terms = Lists.newArrayList("Ravi");
        hoseBirdEndpoint.trackTerms(terms);

        // authentication
        Authentication hoseBirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        // creating the twitterClient
        ClientBuilder clientBuilder = new ClientBuilder()
                .name("Kafka-HoseBird-Twitter")
                .hosts(hoseBirdHosts)
                .endpoint(hoseBirdEndpoint)
                .authentication(hoseBirdAuth)
                .processor(new StringDelimitedProcessor(msgQueue));

        return clientBuilder.build();
    }

    private static KafkaProducer<String, String> createProducer() {

        String bootstrapServer = "localhost:9092";

        // producer config
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // safe producer config
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // high throughput config
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        return new KafkaProducer<String, String>(config);
    }
}
