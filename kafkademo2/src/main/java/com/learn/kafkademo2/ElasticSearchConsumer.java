package com.learn.kafkademo2;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.google.gson.JsonParser.parseString;

public class ElasticSearchConsumer {

    public static void main(String[] args) throws IOException{

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

        // create new client
        RestHighLevelClient client = createClient();

        // create kafkaConsumer
        KafkaConsumer<String, String> kafkaConsumer = getConsumer();

        boolean runLoop = true;

        while(runLoop) {
            //receive msgs from producer
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            logger.info("Received "+records.count()+" messages");
            for(ConsumerRecord<String, String> record:records) {

            // 2 strategies where unique id is generated for a msg which can be used to make consumer idempotent
                // 1. generic id: id = record.topic()+"_"+record.partition()+"_"+record.offset()

                // 2. twitter specific id since we are using Twitter data
                String id = extractId(record.value());

                // insert data received from kafka into elasticsearch
                IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id)
                        .source(record.value(), XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info(indexResponse.getId());

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            logger.info("Committing the offsets");
            kafkaConsumer.commitSync();
            logger.info("Offsets committed");

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        client.close();
    }

    private static String extractId(String tweet) {

        return parseString(tweet).getAsJsonObject().get("id_str").getAsString();
    }

    private static KafkaConsumer<String, String> getConsumer() {

        String bootstrapServer = "localhost:9092";
        String groupId = "twitter-consumer-group";
        String topic = "twitter_topic";

        //create consumer properties
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable auto commit
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5"); // maximum records that can be polled

        //create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(config);

        //subscribe to the required topic
        kafkaConsumer.subscribe(Arrays.asList(topic));

        return kafkaConsumer;

    }

    private static RestHighLevelClient createClient() {

        String hostname = "kafka-elasticsearch-9420304131.ap-southeast-2.bonsaisearch.net";
        String username = "4uffop3fz9";
        String password = "yueooc6iov";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder clientBuilder = RestClient.builder( new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        return new RestHighLevelClient(clientBuilder);
    }
}
