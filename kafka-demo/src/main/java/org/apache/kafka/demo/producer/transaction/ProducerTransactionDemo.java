package org.apache.kafka.demo.producer.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author jie zhao
 * @date 2022/6/10 23:28
 */
public class ProducerTransactionDemo {

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

        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_id_01");
        Producer<String, String> producer = new KafkaProducer<>(props);

        producer.initTransactions();
        producer.beginTransaction();
        try {
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>("quickstart-events", "hello " + i));
            }
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
        } finally {
            producer.close();
        }
    }
}
