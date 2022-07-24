package net.codingw.rocketmq.orderconsumer;

import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public interface ConsumeMessageQueueService {
    void execute(List<MessageExt> consumerRecords);
}
