package org.kafka.conduktor.demos.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoCooperativeRebalance {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoCooperativeRebalance.class.getSimpleName());

    public static void main(String[] args) {

        logger.info("I'm a Kafka Consumer");

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-third-application";
        String topic = "demo_java";

        // Creating properties for Kafka Consumer
        Properties consumerProperties = new Properties();

        // set bootstrap.server, key and value deserializer, consumer_groupId, offset_reset properties
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        // creating consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);

        // Get the reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // Adding the shutdown hook - Create a new thread and join it to the main thread
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            logger.info("Detected a shutdown, let's exit by calling consumer.wakeup() method...");
            kafkaConsumer.wakeup(); // When wakeup() method is invoked, poll() method is going to throw an exception

            // Join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join(); // join() method waits until the completion of the called thread to join the calling thread.
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }));

        try {

            // subscribe consumer to our topic(s)
            kafkaConsumer.subscribe(Collections.singletonList(topic));

            // poll for new data
            while (true) {

                logger.info("Polling");

                ConsumerRecords<String, String> consumerRecords = kafkaConsumer
                        .poll(Duration.ofMillis(1000));

          /*  poll() method polls Kafka for data and returns any data immediately if possible.
            Otherwise, it will wait for the duration specified and move to the next line of the code */

                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    logger.info("Key: " + consumerRecord.key() + "\t" + "Value: " + consumerRecord.value() + "\n" +
                            "Partition: " + consumerRecord.partition() + "\t" + "Offset: " + consumerRecord.offset());
                }


            }

        } catch (WakeupException wakeupException){

            // We ignore this exception as this is an expected exception when closing a consumer
            logger.info("Wake up exception!!");

        } catch (Exception exception){

            logger.error("Unexpected Exception: " + exception);

        } finally {

            // This will also commit the offsets if need to be
            kafkaConsumer.close(); // This is going to gracefully close the consumer and close the connection to the Kafka.
            logger.info("The Consumer is now gracefully closed ");
        }


    }
}
