package com.github.hemal507.kafka.tutorial;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    public static void main(String[] args) {
        String bootstrap_server = "127.0.0.1:9092";

        final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            String topic = "first-topic";
            String value = "Hello World " + Integer.toString(i);
            String key = "Key_" + Integer.toString(i);

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first-topic" , key, "Welcome to Kafka");


            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info(" Key " + record.key() + " Value " + record.value() );
                    } else {
                        logger.error("error while producing ", e);
                    }

                }
            });
        }
            producer.flush();
            producer.close();


    }
    }

