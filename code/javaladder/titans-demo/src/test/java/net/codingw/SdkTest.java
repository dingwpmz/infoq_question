package net.codingw;

import com.alibaba.fastjson.JSON;
import com.zto.Zms;
import com.zto.common.SimpleMessageBuilder;
import com.zto.common.ZmsConst;
import com.zto.consumer.KafkaMessageListener;
import com.zto.consumer.MsgConsumedStatus;
import com.zto.consumer.RocketmqMessageListener;
import com.zto.producer.SendResponse;
import com.zto.producer.ZmsCallBack;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

public class SdkTest {

    @Before
    public void setup() {
        System.setProperty(ZmsConst.ZK.ZMS_STARTUP_PARAM, "10.7.75.59:2181");
//        System.setProperty(ZmsConst.ZK.ZMS_STARTUP_PARAM, "10.9.56.83:2181");
//        System.setProperty(ZmsConst.ZK.ZMS_STARTUP_PARAM, "10.9.20.100:2181,10.9.20.101:2181,10.9.20.65:2181");
    }

    //
//    对于rocketmq来说,有key和无key是不一样的.没有key, retries参数会生效.有key的时候,retires参数不生效,不会做重试.
    @Test
    public void testSendRocketmq() {
        for (int i = 0; i < 1000; i++) {
            Properties properties = new Properties();
            properties.put("timeout", 30);
            properties.put("retries", 0);

            SendResponse response = Zms.send("TestSyncer", new SimpleMessageBuilder().buildPayload((System.currentTimeMillis() + "").getBytes()).buildTags("122").build(), properties);
            System.out.println(response.getCode() + response.getMsgId() + response.getMsg());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testAsyncSendRocketmq() {
            Properties properties = new Properties();
            properties.put("timeout", 100);
            properties.put("retries", 0);

            Zms.sendAsync("TestSyncer", new SimpleMessageBuilder().buildPayload((System.currentTimeMillis() + "").getBytes()).buildTags("122").build(), properties, new ZmsCallBack() {
                @Override
                public void onException(Throwable exception) {
                    System.out.println(exception);
                    System.out.println("1");

                }

                @Override
                public void onResult(SendResponse response) {
                    System.out.println("2");

                    System.out.println(response.getCode() + response.getMsgId() + response.getMsg());

                }
            });


//            Zms.sendAsync("TestSyncer", new SimpleMessageBuilder().buildPaylod((System.currentTimeMillis() + "").getBytes()).buildTags("122").build(), new ZmsCallBack() {
//                @Override
//                public void onException(Throwable exception) {
//                    System.out.println(exception);
//                    System.out.println("1");
//                }
//
//                @Override
//                public void onResult(SendResponse response) {
//                    System.out.println("2");
//                    System.out.println(response.getCode() + response.getMsgId() + response.getMsg());
//
//                }
//            });
            try {
                Thread.sleep(100000000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

    }


    @Test
    public void testSendWithoutProperties() {
        for (int i = 0; i < 1000; i++) {
            SendResponse response = Zms.send("TestSyncer", new SimpleMessageBuilder().buildPayload((System.currentTimeMillis() + "").getBytes()).buildDelayLevel(MsgConsumedStatus.RETRY_10S.getLevel()).buildKey("111").buildTags("122").build());
            System.out.println(response.getInfo());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testSubscribeRocketmq() {
        Zms.subscribe("TestSyncer", new RocketmqMessageListener() {

            public MsgConsumedStatus onMessage(List<MessageExt> msg) {
                for (MessageExt messageExt : msg) {
                    System.out.println(System.currentTimeMillis() - new Long(new String(messageExt.getBody())));
                }

                return MsgConsumedStatus.SUCCEED;
            }
        });

        try {
            Thread.sleep(1000 * 1000 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testSendKafka() {
        for (int i = 0; i < 500; i++) {
            Properties properties = new Properties();
            properties.put("timeout", 120);
            properties.put("acks", "1");
            properties.put("retries", 1);

            SendResponse response = Zms.send("Test_Topic_Kafka_0324_001", new SimpleMessageBuilder().buildPayload((System.currentTimeMillis() + "").getBytes()).build(), properties);
            System.out.println(response.getCode() + response.getMsgId() + response.getMsg());
            try {
                Thread.sleep(1000);


            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testSubscribeKafka() {
        Zms.subscribe("Test_Consumer_Kafka_0324_001_1", new KafkaMessageListener() {


            @Override
            public MsgConsumedStatus onMessage(ConsumerRecord msg) {
                String str = new String((byte[]) msg.value());

                List<MessagePushVO> LL=JSON.parseArray(str,MessagePushVO.class);
                System.out.println(str);


                return MsgConsumedStatus.SUCCEED;
            }
        });

        try {
            Thread.sleep(1000 * 1000 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
