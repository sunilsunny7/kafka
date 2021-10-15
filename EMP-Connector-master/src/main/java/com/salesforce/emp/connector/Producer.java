package com.salesforce.emp.connector;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Producer
{
    public static void main(String[] args)
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "server1.teliacompany.net:9091, server2.teliacompany.net:9092");
        props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
 
        // settings for authentication
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, "myuser", "mypassword");
        props.put("sasl.jaas.config", jaasCfg);
 
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        try
        {
            RecordMetadata output = producer.send(new ProducerRecord<String, String>("mytopic", "mymessage1")).get();
            System.out.printf("timestamp = %d, topic = %s, offset = %s%n", output.timestamp(), output.topic(), output.offset());
 
            output = producer.send(new ProducerRecord<String, String>("mytopic", "mymessage2", "mykey")).get();
            System.out.printf("timestamp = %d, topic = %s, offset = %s%n", output.timestamp(), output.topic(), output.offset());
 
            producer.flush();
            producer.close();
        }
        catch (Exception ex)
        {
            System.out.println(ex.getMessage());
        }
    }
}
