package net.codingw.rocketmq.orderconsumer;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerMessageQueueConcurrentlyService extends AbstractConsumeMessageService{

    private AtomicInteger nextId = new AtomicInteger(0);

    private static final int reset = Integer.MAX_VALUE - 100000;

    public ConsumerMessageQueueConcurrentlyService(DefaultMQLitePushConsumer consumer, MessageQueue messageQueue) {
        super(consumer, messageQueue);
    }

    @Override
    protected int selectTaskQueue(MessageExt msg, int taskQueueTotal) {
        int next = nextId.incrementAndGet();
        if(next > reset ) {
            nextId.set(0);
        }
        return Math.abs(next % taskQueueTotal);
    }
}
