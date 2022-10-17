package org.kafka.conduktor.demos;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {

        logger.info("I'm a Kafka Producer");

        // Create Producer Properties
        Properties producerProperties = new Properties();

        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // Create the Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties);


        for (int i = 0; i < 10; i++) {

            // Create a Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello World!!" + i);

            // Send the data - asynchronous and do Callback
            // This call back is invoked only after successful completion i.e. when a message is successfully completed and sent to Apache Kafka
            kafkaProducer.send(producerRecord, (metadata, exception) -> {

                // This method is executed every time a record is successfully sent or an exception is thrown

                if (exception == null) {
                    // The record was successfully sent

                    logger.info("Received new Metadata \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp());
                } else {
                    logger.error("Error while producing " + exception);
                }
            });

            try{
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        //  Flush data - asynchronous
        kafkaProducer.flush(); // This method will actually block the code at this line and waits until all the data in the above producer being sent is completed

        // Flush and close the Producer
        kafkaProducer.close(); // This close() method also will internally call flush() method. So, the above flush() method is optional

    }
}
