package net.codingw.rocketmq.orderconsumer;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumeMessageQueueServiceFactory {

    private static final ConcurrentHashMap<MessageQueue, ConsumeMessageQueueService> consumeMessageServiceTable = new ConcurrentHashMap<>();

    public static ConsumeMessageQueueService getOrCreateConsumeMessageService(DefaultMQLitePushConsumer consumer, MessageQueue messageQueue, boolean isOrderConsumerModel) {
        return consumeMessageServiceTable.computeIfAbsent(messageQueue, key -> {
            return createConsumeMessageService(consumer, messageQueue, isOrderConsumerModel);
        });
    }

    public static ConsumeMessageQueueService getOrCreateConsumeMessageService(DefaultMQLitePushConsumer consumer, MessageQueue messageQueue, boolean isOrderConsumerModel, Set<MessageQueue> lastAssinQueueSet) {

        return consumeMessageServiceTable.computeIfAbsent(messageQueue, key -> {
            if(lastAssinQueueSet != null) {
                lastAssinQueueSet.add(messageQueue);
            }
            return createConsumeMessageService(consumer, messageQueue, isOrderConsumerModel);
        });
    }

    public static ConsumeMessageQueueService remove(MessageQueue messageQueue) {
        return consumeMessageServiceTable.remove(messageQueue);
    }

    private static ConsumeMessageQueueService createConsumeMessageService(DefaultMQLitePushConsumer consumer, MessageQueue messageQueue, boolean isOrderConsumerModel) {
        if(isOrderConsumerModel) {
            return new ConsumeMessageQueueOrderlyService(consumer,messageQueue);
        } else {
            return new ConsumerMessageQueueConcurrentlyService(consumer, messageQueue);
        }
    }


}
