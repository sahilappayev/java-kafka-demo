package com.kafka.demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class ProducerDemo {

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // send data
        Scanner scanner = new Scanner(System.in);
        int i = 0;
        while (true) {
            if (i == 3) i = 0;
            String key = "key_" + i++;
            System.out.println("Enter the value you want to send:");
            String value = scanner.nextLine();
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("first_topic", key, value);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("====================================================");
                        System.out.println("Key: " + key + ", partition: " + metadata.partition());
                        System.out.println("Metadata." +
                                "\ntopic: " + metadata.topic() +
                                "\npartition: " + metadata.partition() +
                                "\noffset: " + metadata.offset() +
                                "\ntimestamp: " + metadata.timestamp());
                        System.out.println("====================================================");
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }

    }
}
