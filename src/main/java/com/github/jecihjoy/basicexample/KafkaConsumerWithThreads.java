package com.github.jecihjoy.basicexample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaConsumerWithThreads {

    public static void main(String[] args) {
        new KafkaConsumerWithThreads().run();
    }

    private KafkaConsumerWithThreads() {
    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(KafkaConsumerWithThreads.class);

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "test_topic";
        String group_d = "my_group";

        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Starting consumer runnable");
        Runnable consumerRunnable = new ConsumerRunnable(
                latch, bootstrapServers, topic, group_d);

        //starting the thread
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        //shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Inside shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            }catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        }catch (InterruptedException e) {
            logger.error("Application got interrupted " + e);
        }finally {
            logger.info("Application shutting down");
        }

    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(CountDownLatch latch, String bootstrapServers, String topic, String groupId) {
            this.latch = latch;

            //create consumer configs
            Properties prop = new Properties();
            prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //create kafka consumer
            consumer = new KafkaConsumer<String, String>(prop);
            //subscribe consumers to topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            //poll for new data
            try {
                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records){
                        logger.info("key : " + record.key() + " value : "+record.value());
                        logger.info("Partition: " + record.partition() + " Offset: "+record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Shutting down due to WakeupException");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            //to interrupt consumer.poll(). It will throw WakeUpException
            consumer.wakeup();
        }
    }
}

