package com.github.jecihjoy.twitterproducer;

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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import com.github.jecihjoy.util.TwitterConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class TwitterProducer {
    Log log = LogFactory.getLog(TwitterProducer.class);

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        /**blocking queues: Be sure to size these properly based on expected TPS (Transaction per second) of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        /**create a twitter client*/
        Client client = createTwitterClient(msgQueue);
        client.connect();

        /**create a kafka producer*/
        KafkaProducer<String, String>  kafkaProducer = createKafkaProducer();

        /**Add shutdown hook*/
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("SHUTTING DOWN APPLICATION...");
            client.stop();
            log.info("Closing producer");
            kafkaProducer.close();
        }));

        //loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = "";
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
                kafkaProducer.send(new ProducerRecord<>("tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            log.info("Received new metadata. \n" +
                                    "Topic:" + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                log.info("Titter message "+msg);
            }

            log.info("End of application");
        }

    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants
                .STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("Blockchain");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(TwitterConfigs.CONSUMER_KEY, TwitterConfigs.CONSUMER_SECRET, TwitterConfigs.TOKEN, TwitterConfigs.SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01") // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return  builder.build(); //the hosebirdClient
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServers = "127.0.0.1:9092";

        //create producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        return producer;
    }


    /**
     * creating tweets topic
     * kafka-topics --zookeeper 127.0.0.1:2181 --create --topic tweets --partitions 6 --replication-factor 1
     * consuming tweets
     * kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic tweets --group tweets-group
     */
}
