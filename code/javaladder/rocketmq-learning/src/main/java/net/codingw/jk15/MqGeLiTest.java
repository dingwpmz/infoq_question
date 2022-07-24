package net.codingw.jk15;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MqGeLiTest {

    private static final String COLOR_SYS_PROP = "color";

    private static final String COLOR_ENV = System.getProperty(COLOR_SYS_PROP, "");

    public static void main(String[] args) throws  Exception{

        DefaultMQProducer producer = new DefaultMQProducer("dw_test_mq_producer_group");

        //这里省略producer对象的初始化代码
        Message msg = new Message("TOPIC_A", "Hello Topic A".getBytes());
        //设置用户定义的扩展属性，这里是RocketMQ提供的消息属性扩展机制
        msg.putUserProperty("color", "BLUE");
        producer.send( msg);


        // kafka的生产者构建代码省略
        Map<String, String> producerConfig = new HashMap<>();
        KafkaProducer kafkaProducer = new KafkaProducer(producerConfig);
        List<RecordHeader> recordHeaders = new ArrayList<>();
        RecordHeader colorHeader = new RecordHeader("color", "GREEN".getBytes());
        recordHeaders.add(colorHeader);
        ProducerRecord record = new ProducerRecord("TOPIC_A", 0, null, "Hello Topic A".getBytes(), recordHeaders.iterator());
        kafkaProducer.send(record);

    }

}
