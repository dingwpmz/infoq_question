package net.codingw.kafka.learning;


import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 优雅的创建消费组
 */
public class CreateConsumerCmd {

    public static void main(String[] args) {

        createConsumerGroup("localhost:9092", "dw_test_consumer_2022030502", "dw_test_2021112701");

    }


    public static void createConsumerGroup(String bootstapServers, String consumerGroup, String subscribeTopics) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstapServers);
        props.setProperty("group.id", consumerGroup);
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        AtomicBoolean isRunning = new AtomicBoolean(true);
        long begin = System.currentTimeMillis();
        consumer.subscribe(Arrays.asList(subscribeTopics), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("onPartitionsAssigned");
                isRunning.compareAndSet(true, false);
            }
        });

        //拉取消息不消费，缓存一定消息后，将不会拉取新消息，主要的目的是触发消费组重平衡
        System.out.println("等待消费组创建完成");
        while (isRunning.get()) {
            consumer.poll(Duration.ofMillis(100));
        }

        System.out.println("创建成功，耗时" + (System.currentTimeMillis() - begin) + "ms");
    }
}
