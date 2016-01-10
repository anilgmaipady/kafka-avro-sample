package com.test.avro;

public interface KafkaProperties {
    String ZK_CONNECT = "127.0.0.1:2181";
    String GROUP_ID = "group1";
    String TOPIC = "test1";
    String KAFKA_SERVER_URL = "localhost";
    int KAFKA_SERVER_PORT = 9092;
    int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    int CONNECTION_TIMEOUT = 100000;
    int RECONNECT_INTERVAL = 10000;
    String CLIENT_ID = "SimpleConsumerDemoClient";
}
