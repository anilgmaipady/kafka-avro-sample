package com.test.avro;

import kafka.utils.ShutdownableThread;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumerSchemaInEachMessage {
    private final KafkaConsumer<Integer, byte[]> consumer;
    private final String topic;

    public KafkaAvroConsumerSchemaInEachMessage(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }


    public void fetch() {
        consumer.subscribe(Collections.singletonList(this.topic));
        ConsumerRecords<Integer, byte[]> records = consumer.poll(1000);
        for (ConsumerRecord<Integer, byte[]> record : records) {
            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
        }
    }

    public static void main(String str[]){
        KafkaAvroConsumerSchemaInEachMessage consumerThread =
                new KafkaAvroConsumerSchemaInEachMessage("test");
        consumerThread.fetch();
    }
}