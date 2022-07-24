package net.codingw.rocketmq.orderconsumer;

import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class OrderlyConsumerTask extends AbstractConsumerTask{

    public OrderlyConsumerTask(DefaultMQLitePushConsumer consumer, BlockingQueue<MessageExt> msgQueue, MessageListener messageListener) {
        super(consumer, msgQueue, messageListener);
    }

    protected void doTask(List<MessageExt> msgs) {
        try {
            if (msgs == null || msgs.isEmpty()) {
                return;
            }

            MessageListenerOrderly listener = (MessageListenerOrderly)this.messageListener;
            long begin = System.currentTimeMillis();

            while (true) {
                try {
                    ConsumeOrderlyStatus status = listener.consumeMessage(msgs, null);
                    if (status == ConsumeOrderlyStatus.SUCCESS) {
                        break;
                    } else {
                        TimeUnit.MILLISECONDS.sleep(1000);
                    }
                } catch (Throwable e) {
                    LOGGER.error("顺序消费失败", e);
                }

            }
            // 处理位点
            removeMessageAndCommitOffset(msgs);
        } catch (Throwable ex) {
            LOGGER.error("consume message error", ex);
        }
    }
}
