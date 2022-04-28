package com.learn.kafkademo3;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import static com.google.gson.JsonParser.parseString;

public class KafkaStreamsDemo {

    public static void main(String[] args) {

        String bootstrapServer = "localhost:9092";
        String applicationId = "kafka-streams-demo";
        String inputTopic = "twitter_topic";
        String outputTopic = "filtered_twitter_topic";

        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // inputStream
        KStream<String, String> inputStream = streamsBuilder.stream(inputTopic);

        // outputStream filter
        KStream<String, String> filteredStream = inputStream.filter(
                (k, v) -> getFollowers(v) > 10000
        );

        // OutputStream to outputTopic
        filteredStream.to(outputTopic);

        // build a topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // start our streams application
        kafkaStreams.start();
    }

    private static Integer getFollowers(String jsonTweet) {

        return parseString(jsonTweet).getAsJsonObject()
                .get("user").getAsJsonObject()
                .get("followers_count").getAsInt();
    }
}
