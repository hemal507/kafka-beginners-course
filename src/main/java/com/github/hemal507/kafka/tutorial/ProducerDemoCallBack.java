package com.github.hemal507.kafka.tutorial;


import jdk.nashorn.internal.codegen.CompilerConstants;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


import java.util.Properties;

public class ProducerDemoCallBack {
    public static void main(String[] args) {
        String bootstrap_server =  "127.0.0.1:9092" ;

        final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_server);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        final ProducerRecord<String,String> record = new ProducerRecord<String, String>("first-topic","Welcome to Kafka");


        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e==null) {
                    logger.info(" Key " + record.key() + " Value "  + record.value());
                } else {
                    logger.error("error while producing " , e);
                }

            }
        });

        producer.flush();
        producer.close();

        }
    }

