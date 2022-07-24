package net.codingw.rocketmq.orderconsumer;

import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public abstract class AbstractConsumerTask implements Runnable{

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractConsumerTask.class);

    protected volatile boolean isRunning = true;
    // 可在一个线程中执行的消息
    protected BlockingQueue<MessageExt> msgQueue;
    protected MessageListener messageListener;

    protected DefaultMQLitePushConsumer consumer;


    public AbstractConsumerTask(DefaultMQLitePushConsumer consumer, BlockingQueue<MessageExt> msgQueue, MessageListener messageListener) {
        this.consumer = consumer;
        this.msgQueue = msgQueue;
        this.messageListener = messageListener;
    }


    public void run() {
        try {
            while (isRunning) {
                try {
                    //判断是否是批量消费
                    List<MessageExt> msgs = new ArrayList<>(this.consumer.getConsumeBatchSize());
                    while(msgQueue.drainTo(msgs, this.consumer.getConsumeBatchSize()) <= 0 ) {
                        Thread.sleep(20);
                    }
                    doTask(msgs);
                } catch (InterruptedException e) {
                    LOGGER.info(Thread.currentThread().getName() + "is Interrupt");
                    break;
                } catch (Throwable e) {
                    LOGGER.error("consume message error", e);
                }
            }
        } catch (Throwable e) {
            LOGGER.error("consume message error", e);
        }
    }

    protected abstract void doTask(List<MessageExt> msgs);

    public void stop() {
        this.isRunning = false;
    }

    /**
     * 移除消息，并提交位点
     * @param msgs
     */
    protected void removeMessageAndCommitOffset(List<MessageExt> msgs) {
        for(MessageExt msg : msgs) {
            removeMessageAndCommitOffset(msg);
        }
    }

    protected void removeMessageAndCommitOffset(MessageExt msg) {
        MessageQueue messageQueue = new MessageQueue(msg.getBrokerName(), msg.getTopic(), msg.getQueueId());
        AbstractConsumeMessageService consumeMessageService = (AbstractConsumeMessageService) ConsumeMessageQueueServiceFactory.getOrCreateConsumeMessageService(consumer, messageQueue, consumer.isOrderConsumerModel());
        long offset = consumeMessageService.removeMessage(msg);
        consumer.addOffset(messageQueue, offset);
        // 进行恢复
        consumeMessageService.maybeNeedResume();
    }
}
