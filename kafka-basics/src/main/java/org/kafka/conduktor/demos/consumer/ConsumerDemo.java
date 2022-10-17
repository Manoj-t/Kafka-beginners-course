package org.kafka.conduktor.demos.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        logger.info("I'm a Kafka Consumer");

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-second-application";
        String topic = "demo_java";

        // Creating properties for Kafka Consumer
        Properties consumerProperties = new Properties();

        // set bootstrap.server, key and value deserializer, consumer_groupId, offset_reset properties
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // creating consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);

        // subscribe consumer to our topic(s)
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        // poll for new data
        while (true){

            logger.info("Polling");

            ConsumerRecords<String, String> consumerRecords = kafkaConsumer
                    .poll(Duration.ofMillis(1000));
          /*  poll() method polls Kafka for data and returns any data immediately if possible.
            Otherwise, it will wait for the duration specified and move to the next line of the code*/

            for(ConsumerRecord<String, String> consumerRecord : consumerRecords){
                logger.info("Key: " + consumerRecord.key() + "\t" + "Value: " + consumerRecord.value() + "\n" +
                        "Partition: " + consumerRecord.partition() + "\t" + "Offset: " + consumerRecord.offset() );
            }


        }






    }
}
