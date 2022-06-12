package org.apache.kafka.demo.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author jie zhao
 * @date 2022/6/10 23:28
 */
public class ProducerPartitionerDemo {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 可选配置项
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("quickstart-events",
                    0,
                    "",
                    "hello " + i),
                    (metadata, exception) -> {
                        if (exception == null) {
                            System.out.println("发送主题：" + metadata.topic() + ", 发送分区：" + metadata.partition());
                        }
                    });
        }
        producer.close();
    }
}
