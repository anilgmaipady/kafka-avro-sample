package com.test.avro;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaAvroPublisherSchemaInEachMessage {
    public static void main(String str[]) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "KafkaAvroPublisherSchemaInEachMessageProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        KafkaProducer<Integer, byte[]> producer = new KafkaProducer<Integer, byte[]>(props);
        try {

            Activity activity = new Activity();
            String randomString = java.util.UUID.randomUUID().toString();
            activity.setId(randomString);
            activity.setName(randomString);
            activity.setDescription(randomString);
            byte avroBytes[] = ObjectToAvro.marshal(activity);
            producer.send(new ProducerRecord<Integer, byte[]>("test1", 1, avroBytes)).get();
            Thread.sleep(1000);
            producer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}