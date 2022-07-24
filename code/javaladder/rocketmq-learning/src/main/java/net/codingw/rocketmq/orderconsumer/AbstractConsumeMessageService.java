package net.codingw.rocketmq.orderconsumer;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractConsumeMessageService implements ConsumeMessageQueueService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConsumeMessageService.class);

    private int allowPerPartitionMaxConsumeRecords = Integer.parseInt(System.getProperty("allow.per.partition.max.records", "2000"));

    private DefaultMQLitePushConsumer consumer;


    /**
     * 每一个队列对应一个ConsumerMessageConcurrentlyService对象
     */
    protected MessageQueue messageQueue;
    private long queueMaxOffset = -1;

    protected final AtomicBoolean started = new AtomicBoolean(false);

    protected ReentrantReadWriteLock msgTreeMapLock = new ReentrantReadWriteLock();
    protected ReentrantReadWriteLock.ReadLock msgTreeMapReadLock = msgTreeMapLock.readLock();
    protected ReentrantReadWriteLock.WriteLock msgTreeMapWriteLock = msgTreeMapLock.writeLock();
    // 用于实现最小消费位点提交
    protected final TreeMap<Long/** msg offset*/, MessageExt> msgTreeMap = new TreeMap<>();

    public AbstractConsumeMessageService(DefaultMQLitePushConsumer consumer, MessageQueue messageQueue) {
        this.consumer = consumer;
        this.messageQueue = messageQueue;
    }


    @Override
    public void execute(List<MessageExt> consumerRecords) {
        if (consumerRecords == null || consumerRecords.isEmpty()) {
            return;
        }

        // 将消息放入到待消费队列中，无解队列
        putMessage(consumerRecords);

        if (isNeedPause()) {
            consumer.getConsumerLimitController().addPausePartition(messageQueue);
        }

        for (MessageExt msg : consumerRecords) {
            int taskIndex = selectTaskQueue(msg, consumer.getTaskQueueSize());
            try {
                consumer.submitMessage(taskIndex, msg);
            } catch (Throwable e) {
                // ignore e
                e.printStackTrace();
            }
        }

    }

    protected abstract int selectTaskQueue(MessageExt msg, int taskQueueTotal);

    protected Long removeMessage(MessageExt msg) {
        long result = -1;
        try {
            msgTreeMapWriteLock.lock();
            if (!msgTreeMap.isEmpty()) {
                result = this.queueMaxOffset + 1;
                msgTreeMap.remove(msg.getQueueOffset());
                if (!msgTreeMap.isEmpty()) {
                    result = msgTreeMap.firstKey();
                }
            }
        } finally {
            msgTreeMapWriteLock.unlock();
        }
        return result;
    }

    protected Long removeMessage(List<MessageExt> msgs) {
        long result = -1;
        try {
            msgTreeMapWriteLock.lock();
            if (!msgTreeMap.isEmpty()) {
                result = this.queueMaxOffset + 1;
                for (MessageExt msg : msgs) {
                    msgTreeMap.remove(msg.getQueueOffset());
                }

                if (!msgTreeMap.isEmpty()) {
                    result = msgTreeMap.firstKey();
                }
            }
        } finally {
            msgTreeMapWriteLock.unlock();
        }
        return result;
    }

    protected void putMessage(List<MessageExt> msgs) {
        try {
            msgTreeMapWriteLock.lock();
            msgs.forEach((msg) -> {
                MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                if (old == null) {
                    this.queueMaxOffset = msg.getQueueOffset();
                }
            });
        } finally {
            msgTreeMapWriteLock.unlock();
        }
    }

    protected void maybeNeedResume() {
        // 进行恢复
        if (isNeedResume()) {
            consumer.getConsumerLimitController().addResumePartition(messageQueue);
        }
    }

    /**
     * 是否需要暂停拉取
     *
     * @return true:限流;false：不限流
     */
    protected boolean isNeedPause() {
        this.msgTreeMapReadLock.lock();
        try {
            if (msgTreeMap.isEmpty()) {
                return false;
            }
            // 数据达到一定限度，停止该分区消费（即停止 rocketmq 向 Broker 拉取消息）
            return msgTreeMap.size() > allowPerPartitionMaxConsumeRecords || (msgTreeMap.lastKey() - msgTreeMap.firstKey()) > allowPerPartitionMaxConsumeRecords;
        } finally {
            this.msgTreeMapReadLock.unlock();
        }
    }

    /**
     * 是否恢复拉取
     *
     * @return true：取消限流;false:保持限流状态
     */
    protected boolean isNeedResume() {
        this.msgTreeMapReadLock.lock();
        try {
            if (msgTreeMap.isEmpty()) {
                return true;
            }

            // 数据达到一定限度，停止该分区消费（即停止 rocketmq 向 Broker 拉取消息）
            if (msgTreeMap.size() < allowPerPartitionMaxConsumeRecords / 2) {
                return true;
            }

            return false;
        } finally {
            this.msgTreeMapReadLock.unlock();
        }
    }


}
