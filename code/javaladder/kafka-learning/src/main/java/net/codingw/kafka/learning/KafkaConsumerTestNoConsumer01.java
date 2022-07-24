package net.codingw.kafka.learning;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class KafkaConsumerTestNoConsumer01 {

    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "dw_test_consumer_2021112701");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("dw_test_2021112701"));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

//        int i = 100000;
//        while ( true ) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
////            System.out.printf("records:" + records.isEmpty());
//            for (ConsumerRecord<String, String> record : records) {
//                buffer.add(record);
//            }
//            if (buffer.size() > 0) {
//                doSomething(buffer);
//                consumer.commitAsync();
//                buffer.clear();
//            }
//        }

        System.out.println("不消费，就等着，看是否发生神奇的现象，期待中。。。。。");
        CountDownLatch cdh = new CountDownLatch(1);
        cdh.await();




    }


    private static boolean doSomething(List<ConsumerRecord<String, String>> buffer) {

        System.out.printf("aa");

        return true;
    }


    /**
     * 执行用户注册的消费监听器
     * @param record
     * @return
     */
    private Map<String, Object> doConsumer(ConsumerRecord record) {
        return null;
    }

    /**
     * 消费端任务
     */
    class ConsumerTask implements Runnable {
        TopicPartition topicPartition;
        List<ConsumerRecord<String, String>> records;
        public ConsumerTask(TopicPartition topicPartition, List<ConsumerRecord<String, String>> records) {
            this.topicPartition = topicPartition;
            this.records = records;
        }

        @Override
        public void run() {
            if(records == null || records.isEmpty()) {
                // 输出告警并返回
                return;
            }
            synchronized (topicPartition) {
                // 对分区加锁，这里只是demo,这里最好别对该对象加锁，而是维护一个 <TopicPartition, Lock>
                // 因为TopicPartition只是 topic-分区对元信息，可能会反复创建
                for(ConsumerRecord record : records) {
                    doConsumer(record);
                }
            }

        }
    }




}
