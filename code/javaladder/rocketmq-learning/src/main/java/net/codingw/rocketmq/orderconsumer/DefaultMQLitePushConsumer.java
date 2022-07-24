package net.codingw.rocketmq.orderconsumer;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 基于RocketMQ DefaultMQLitePullConsumer 构建的PUSH消费者
 */
public class DefaultMQLitePushConsumer {

    protected static final Logger LOGGER = LoggerFactory.getLogger(DefaultMQLitePushConsumer.class);

    /** 消息拉取间隔*/
    private long consumerPollTimeoutMs = Long.parseLong(System.getProperty("consumer.poll.timeout.ms", "100"));
    /** PULL线程数量*/
    private int pullTaskThreadCount = Integer.parseInt(System.getProperty("pull.task.thread.count", String.valueOf(Runtime.getRuntime().availableProcessors() * 2)));
    /** 消费位点提交间隔，默认 5s*/
    private int persistConsumerOffsetInterval =  Integer.parseInt(System.getProperty("persist.offset.interval", "5000"));
    /** 一次提交消息消费的条数，默认为1*/
    private int consumeBatchSize = Integer.parseInt(System.getProperty("consume.batch.size", "1"));
    /** 消费限流控制器*/
    private final ConsumerLimitController consumerLimitController = new ConsumerLimitController();
    /** RocketMQ Lite pull 消费者对象*/
    private DefaultLitePullConsumer consumer;
    /** nameserver地址*/
    private String nameServerAddr;
    /** 消费组名称*/
    private String consumerGroup;
    /** pull线程消息数量*/
    private int pullBatchSize = pullTaskThreadCount;
    /** 消费者线程数量*/
    private int consumerThreadCount = 20;
    /** 初次消费位点查找策略*/
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    /** 消费模式*/
    private MessageModel consumeMessageModel = MessageModel.CLUSTERING;

    /** 线程池*/
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "RocketMqLitePushConsumerScheduledThread"));

    /** 事件监听器*/
    private MessageListener messageListener;
    /** 是否是顺序消费*/
    private boolean isOrderConsumerModel = false;
    /** 订阅主题*/
    private String topic;
    /** 订阅的tag*/
    private String tags;

    /** 上一次分配的队列集合*/
    private final Set<MessageQueue> lastAssignSet = Collections.synchronizedSet(new HashSet<>());
    /** 上一次分配到的重试队列集合*/
    private final Set<MessageQueue> lastRetryAssignSet = Collections.synchronizedSet(new HashSet<>());

    /** 管理命令客户端*/
    private DefaultMQAdminExt defaultMQAdminExt;

    /** 消费组线程池*/
    private ThreadPoolExecutor consumerThreadGroup;
    /** 消费组任务队列 */
    private List< BlockingQueue > msgByKeyBlockQueue;
    /** 消费组任务运行任务*/
    private List<AbstractConsumerTask> consumerRunningTasks;

    /** 是否是运行中*/
    private volatile boolean isRunning = false;





    /**
     * 为了简单，暂时只支持订阅一个topic
     * @param nameServerAddr
     * @param consumerGroup
     * @param topic
     * @param tags
     */
    public DefaultMQLitePushConsumer(String nameServerAddr, String consumerGroup, String topic,String tags) {
        this.nameServerAddr = nameServerAddr;
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.tags = tags;
    }


    /** pubblic method start */
    public void registerMessageListener(MessageListenerConcurrently messageListener) {
        this.isOrderConsumerModel = false;
        this.messageListener = messageListener;
    }

    /**
     * Register a callback to execute on message arrival for orderly consuming.
     *
     * @param messageListener message handling callback.
     */
    public void registerMessageListener(MessageListenerOrderly messageListener) {
        this.isOrderConsumerModel = true;
        this.messageListener = messageListener;
    }

    public void submitMessage(int index, MessageExt msg) throws InterruptedException {
        msgByKeyBlockQueue.get(index).put(msg);

    }

    /**
     * @param messageQueue 消息队列
     * @param offset TreeMap 中最小的位点，为-1表示正在处理的消息
     */
    public void addOffset(MessageQueue messageQueue, Long offset) {
        if(offset < 0 ) {
            return;
        }
        this.consumer.getOffsetStore().updateOffset(messageQueue, offset, true);
    }

    public void start(){
        consumer = new DefaultLitePullConsumer(this.consumerGroup);
        try {
            initMqAdminAndStart();

            consumer.setNamesrvAddr(this.nameServerAddr);
            buildClientId();
            consumer.setVipChannelEnabled(false);
            consumer.setAutoCommit(false);
            consumer.setPullThreadNums(pullTaskThreadCount);
            consumer.setConsumeFromWhere(consumeFromWhere);
            consumer.setMessageModel(consumeMessageModel);

            consumer.subscribe(this.topic, this.tags);
            // 订阅重试消息
            switch (this.consumer.getMessageModel()) {
                case BROADCASTING:
                    break;
                case CLUSTERING:
                    String retryTopic = MixAll.getRetryTopic(this.consumer.getConsumerGroup());
                    consumer.subscribe(retryTopic, "*");
                    break;
                default:
                    break;
            }

            MessageQueueListener messageQueueListener = consumer.getMessageQueueListener();
            consumer.setMessageQueueListener((topic, mqAll, mqDivided) -> {
                messageQueueListener.messageQueueChanged(topic, mqAll, mqDivided);
                messageQueueOnChange(topic, mqDivided, consumeFromWhere);
            });
        } catch (MQClientException e) {
            LOGGER.error("RocketMQConsumer register {} error", this.topic, e);
            throw new RuntimeException(e);
        }

        // 启动消费组线程池
        startConsumerThreads();

        try {
            consumer.start();
            LOGGER.info("Consumer started at {}, consumer group name:{}", System.currentTimeMillis(),this.topic);
        } catch (Exception e) {
            LOGGER.error("RocketMQConsumer start error", e);
        }

        //启动PULL线程
        startPullThread();

        //开启定时任务提交位点
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                persistConsumerOffset();
            } catch (Exception e) {
                LOGGER.error("ScheduledTask persistAllConsumerOffset exception", e);
            }
        }, 1000 * 10, persistConsumerOffsetInterval, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        this.consumer.shutdown();
        this.consumerThreadGroup.shutdown();
        scheduledExecutorService.shutdown();
        this.stopPullThread();
    }

    private void startPullThread() {
        {
            String threadName = "Lite-Push-Pull-Service-" + this.consumer + "-" + LocalDateTime.now();
            Thread litePushPullService = new Thread(() -> {
                try {
                    while (isRunning) {
                        List<MessageExt> records = consumer.poll(consumerPollTimeoutMs);
                        submitRecords(records);
                        consumerLimitController.pause();
                        consumerLimitController.resume();
                    }
                } catch (Throwable ex) {
                    LOGGER.error("consume poll error", ex);
                } finally {
                    stopPullThread();
                }
            }, threadName);
            litePushPullService.start();
            LOGGER.info("Lite Push Consumer started at {}, consumer group name:{}", System.currentTimeMillis(), this.consumerGroup);
        }
    }


    private void submitRecords(List<MessageExt> records) {
        if (records == null || records.isEmpty()) {
            return;
        }
        MessageExt firstMsg = records.get(0);
        MessageQueue messageQueue = new MessageQueue(firstMsg.getTopic(), firstMsg.getBrokerName(), firstMsg.getQueueId());
        // 根据队列获取队列级别消费服务类
        ConsumeMessageQueueService tempConsumeMessageService = ConsumeMessageQueueServiceFactory.getOrCreateConsumeMessageService(this, messageQueue, isOrderConsumerModel, lastAssignSet);
        tempConsumeMessageService.execute(records);
    }

    // 启动消费组线程池
    private void startConsumerThreads() {
        String threadPrefix = isOrderConsumerModel ? "OrderlyConsumerThreadMessage_" : "ConcurrentlyConsumerThreadMessage_";
        AtomicInteger threadNumIndex = new AtomicInteger(0);
        consumerThreadGroup = new ThreadPoolExecutor(consumerThreadCount, consumerThreadCount, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> {
            Thread t = new Thread(r);
            t.setName(threadPrefix + threadNumIndex.incrementAndGet() );
            return t;
        });

        msgByKeyBlockQueue = new ArrayList(consumerThreadCount);
        consumerRunningTasks = new ArrayList<>(consumerThreadCount);
        for(int i =0; i < consumerThreadCount; i ++ ) {
            msgByKeyBlockQueue.add(new LinkedBlockingQueue());
            AbstractConsumerTask task = null;
            if(isOrderConsumerModel) {
                task = new OrderlyConsumerTask(this, msgByKeyBlockQueue.get(i), this.messageListener);
            } else {
                task = new ConcurrentlyConsumerTask(this, msgByKeyBlockQueue.get(i), this.messageListener);
            }
            consumerRunningTasks.add(task);
            //启动消费线程
            consumerThreadGroup.submit(task);
        }
    }

    private void buildClientId() {
        long now = System.currentTimeMillis();
        consumer.setClientIP("consumer-client-id-" + nameServerAddr + "-" + now);
    }

    private void persistConsumerOffset() {
        try {
            Set<MessageQueue> assignSet = new HashSet<>();
            assignSet.addAll(lastAssignSet);
            this.consumer.getOffsetStore().persistAll(assignSet);

            Set<MessageQueue> retryssignSet = new HashSet<>();
            retryssignSet.addAll(lastRetryAssignSet);
            this.consumer.getOffsetStore().persistAll(retryssignSet);
        } catch (Throwable e) {
            LOGGER.error("提交消费位点失败", e);
        }
    }

    private void initMqAdminAndStart() throws MQClientException{
        defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setNamesrvAddr(this.nameServerAddr);
        defaultMQAdminExt.start();
    }

    /**
     * 关闭pull线程
     */
    private void stopPullThread() {
        this.isRunning = false;
    }


    // 用来保护队列重平衡
    private ReentrantLock allocateLock = new ReentrantLock();
    private void messageQueueOnChange(String topic, Set<MessageQueue> mqDivided, ConsumeFromWhere consumeFromWhere) {
        try {
            allocateLock.lock();

            Set<MessageQueue> optAssignSet = null;

            if(topic.startsWith("%RETRY%")) {
                optAssignSet = lastRetryAssignSet;
            } else {
                optAssignSet = lastAssignSet;
            }

            if (optAssignSet != null && !optAssignSet.isEmpty()) {
                for (MessageQueue mq : optAssignSet) {
                    if (mq.getTopic().equals(topic)) {
                        if (!mqDivided.contains(mq)) { // 之前的队列 不在 最新分配的队列集合中，需释放资源
                            LOGGER.info("MessageQueue: brokerName:{},topic:{},queueId:{} removed", mq.getBrokerName(), mq.getTopic(), mq.getQueueId());
                            ConsumeMessageQueueService consumeMessageService = ConsumeMessageQueueServiceFactory.remove(mq);
                            if (null != consumeMessageService) {
                                optAssignSet.remove(mq);
                            }
                        }
                    }
                }
            }

            TopicStatsTable topicStatsTable = null;
            try {
                topicStatsTable = defaultMQAdminExt.examineTopicStats(topic);
            } catch (Exception e) {
                LOGGER.error("查询路由信息失败", e);
            }

            for (MessageQueue mq : mqDivided) {
                if (!optAssignSet.contains(mq)) {
                    LOGGER.info("MessageQueue: brokerName:{},topic:{},queueId:{} assign", mq.getBrokerName(), mq.getTopic(), mq.getQueueId());
                    try {
                        Long offset = consumer.committed(mq);
                        //设置本地拉取分量，下次拉取消息以这个偏移量为准
                        if (offset == null || offset < 0) {
                            TopicOffset topicOffset = topicStatsTable.getOffsetTable().get(mq);
                            if(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET.equals(consumeFromWhere)) {
                                consumer.seek(mq, topicOffset.getMaxOffset());
                            } else {
                                consumer.seek(mq, topicOffset.getMinOffset());
                            }
                        }
                        optAssignSet.add(mq);
                        ConsumeMessageQueueServiceFactory.getOrCreateConsumeMessageService(this, mq, this.isOrderConsumerModel);

                    } catch (Exception e) {
                        LOGGER.error("consumer seek error", e);
                    }
                }
            }
        } finally {
            allocateLock.unlock();
        }
    }


    /** get and set start */
    public ConsumerLimitController getConsumerLimitController() {
        return consumerLimitController;
    }

    public int getPullTaskThreadCount() {
        return pullTaskThreadCount;
    }

    public void setPullTaskThreadCount(int pullTaskThreadCount) {
        this.pullTaskThreadCount = pullTaskThreadCount;
    }

    public int getConsumeBatchSize() {
        return consumeBatchSize;
    }

    public void setConsumeBatchSize(int consumeBatchSize) {
        this.consumeBatchSize = consumeBatchSize;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public int getConsumerThreadCount() {
        return consumerThreadCount;
    }

    public void setConsumerThreadCount(int consumerThreadCount) {
        this.consumerThreadCount = consumerThreadCount;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public void setConsumeMessageModel(MessageModel consumeMessageModel) {
        this.consumeMessageModel = consumeMessageModel;
    }

    public MessageModel getConsumeMessageModel() {
        return consumeMessageModel;
    }

    public boolean isOrderConsumerModel() {
        return isOrderConsumerModel;
    }

    public int getTaskQueueSize() {
        return msgByKeyBlockQueue == null ? 0 : msgByKeyBlockQueue.size();
    }

    /** get and set end */

    /**
     * 消费限流控制器
     */
    class ConsumerLimitController {
        private final Set<MessageQueue> pausePartitions = new HashSet<>();
        private final Set<MessageQueue> alreadyPausePartitions = new HashSet<>();
        private final Set<MessageQueue> resumePartitions = new HashSet<>();
        private final ReentrantReadWriteLock pauseAndResumeLock = new ReentrantReadWriteLock();
        private final ReentrantReadWriteLock.ReadLock pauseAndResumeReadLock = pauseAndResumeLock.readLock();
        private final ReentrantReadWriteLock.WriteLock pauseAndResumeWriteLock = pauseAndResumeLock.writeLock();


        public void addPausePartition(MessageQueue messageQueue) {
            pauseAndResumeWriteLock.lock();
            try {
                resumePartitions.remove(messageQueue);
                pausePartitions.add(messageQueue);
                LOGGER.warn("MessageQueue consumerGroup:{},brokerName:{}, topic:{},queueId:{} is pause", consumer.getConsumerGroup(), messageQueue.getBrokerName(), messageQueue.getTopic(), messageQueue.getQueueId());
            } finally {
                pauseAndResumeWriteLock.unlock();
            }
        }

        public void addResumePartition(MessageQueue messageQueue) {
            //恢复
            pauseAndResumeReadLock.lock();
            try {
                // 目前队列没有被限流，直接返回
                if(!alreadyPausePartitions.contains(messageQueue)) {
                    return;
                }
            } finally {
                pauseAndResumeReadLock.unlock();
            }

            // 申请写锁
            pauseAndResumeWriteLock.lock();
            try {
                if(alreadyPausePartitions.contains(messageQueue)) {
                    pausePartitions.remove(messageQueue);
                    alreadyPausePartitions.remove(messageQueue);
                    resumePartitions.add(messageQueue);

                    LOGGER.info("MessageQueue consumerGroup:{},brokerName:{}, topic:{},queueId:{} is resume", consumer.getConsumerGroup(), messageQueue.getBrokerName(), messageQueue.getTopic(), messageQueue.getQueueId());
                }

            } finally {
                pauseAndResumeWriteLock.unlock();
            }
        }

        public void pause() {
            pauseAndResumeReadLock.lock();
            try {
                if(!pausePartitions.isEmpty()) {
                    consumer.pause(pausePartitions);
                    alreadyPausePartitions.addAll(pausePartitions);
                    pausePartitions.clear();
                }
            } finally {
                pauseAndResumeReadLock.unlock();
            }
        }

        public void resume() {
            pauseAndResumeReadLock.lock();
            try {
                if(!resumePartitions.isEmpty()) {
                    consumer.resume(resumePartitions);
                    pausePartitions.clear();
                }

            } finally {
                pauseAndResumeReadLock.unlock();
            }
        }
    }






}
