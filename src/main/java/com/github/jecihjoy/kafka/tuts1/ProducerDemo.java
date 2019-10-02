package com.github.jecihjoy.kafka.tuts1;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


public class ProducerDemo {

    public static void main(String[] args) {
        String bootstrapServers = "10.50.201.25:9092";
        //create producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        //create producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("my-topic", "hello world");

        //send data
        producer.send(record);

        producer.flush();

        producer.close();
    }
}
