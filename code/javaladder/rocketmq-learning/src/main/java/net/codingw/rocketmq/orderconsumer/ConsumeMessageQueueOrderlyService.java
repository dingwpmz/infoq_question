package net.codingw.rocketmq.orderconsumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

public class ConsumeMessageQueueOrderlyService extends AbstractConsumeMessageService{

    private final String NO_KEY_HASH = "__nokey";

    public ConsumeMessageQueueOrderlyService(DefaultMQLitePushConsumer consumer, MessageQueue messageQueue) {
        super(consumer, messageQueue);
    }


    @Override
    protected int selectTaskQueue(MessageExt msg, int taskQueueTotal) {
        String keys = msg.getKeys();
        if(StringUtils.isEmpty(keys)) {
            keys = NO_KEY_HASH;
        }
        return  Math.abs(  keys.hashCode()   ) %  taskQueueTotal;
    }
}
