package net.codingw.rocketmq.orderconsumer;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class ConcurrentlyConsumerTask extends AbstractConsumerTask{

    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentlyConsumerTask.class);

    public ConcurrentlyConsumerTask(DefaultMQLitePushConsumer consumer, BlockingQueue<MessageExt> msgQueue, MessageListener messageListener) {
        super(consumer, msgQueue, messageListener);
    }
    @Override
    protected void doTask(List<MessageExt> msgs) {
        try {
            if (msgs == null || msgs.isEmpty()) {
                return;
            }
            MessageListenerConcurrently listener = (MessageListenerConcurrently)this.messageListener;
            ConsumeConcurrentlyStatus status = null;
            try {
                status = listener.consumeMessage(msgs, null);

            } catch (Throwable e) {
                LOGGER.error("并发消费失败，将按重试策略进行重试", e);
            }

            if (status != ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
                sendMessageBack(msgs);
            }
            // 处理位点
            removeMessageAndCommitOffset(msgs);
        } catch (Throwable ex) {
            LOGGER.error("consume message error", ex);
        }
    }

    /**
     * 将消息重新发送到服务端
     *
     * @param msgs   需要重试的消息
     */
    private void sendMessageBack(List<MessageExt> msgs) {
        switch (this.consumer.getConsumeMessageModel()) {
            case BROADCASTING:
                for (int i = 0; i < msgs.size(); i++) {
                    MessageExt msg = msgs.get(i);
                    LOGGER.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            case CLUSTERING:
                for (int i = 0; i < msgs.size(); i++) {
                    MessageExt msg = msgs.get(i);
                    try {
                        while (!this.sendMessageBack(msg, msg.getDelayTimeLevel())) {
                            Thread.sleep(1000);
                        }
                    } catch (Throwable e) {
                        LOGGER.error("send message back error", e);
                    }
                }
                break;
            default:
                break;
        }
    }

    private boolean sendMessageBack(final MessageExt msg, int delayLevel) {
        return true;
        // Wrap topic with namespace before sending back message.
//        msg.setTopic(this.consumer.withNamespace(msg.getTopic()));
//        try {
//            // 可以使用消息发送者进行消息发送
//            // @todo 实现消息发送
//            //this.consumer.sendMessageBack(msg, delayLevel);
//            return true;
//        } catch (Exception e) {
//            LOGGER.error("sendMessageBack exception, group: " + this.consumer.getConsumerGroup() + " msg: " + msg.toString(), e);
//        }
//        return false;
    }
}
