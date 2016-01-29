package com.test.avro;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.message.MessageAndMetadata;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.IndexedRecord;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.common.errors.SerializationException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

class KafkaSchemaRegistryJsonToAvroConsumer {


    public static void main(String str[]) {

        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "group1");
        props.put("schema.registry.url", "http://localhost:8081");

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put("test1", new Integer(1));

        VerifiableProperties vProps = new VerifiableProperties(props);
        KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(vProps);
        KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);
/*
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(vProps));

        Map<String, List<KafkaStream>> consumerMap = consumer.createMessageStreams(
                topicCountMap, keyDecoder, valueDecoder);
        KafkaStream stream = consumerMap.get(topic).get(0);
        ConsumerIterator it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata messageAndMetadata = it.next();
            try {
                String key = (String) messageAndMetadata.key();
                String value = (IndexedRecord) messageAndMetadata.message();

            } catch (SerializationException e) {
                // may need to do something with it
            }
        }
*/
    }
}