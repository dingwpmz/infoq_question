package net.codingw;

/**
 * @author yuhao.zhang
 * @description
 * @date 2020/5/21
 */
public class MessagePushVO {

    /**
     * 集群
     */
    private String clusterName;
    /**
     * 指标名称 tps latency
     */
    private String name;
    /**
     * 时间ms
     */
    private Long timestamp;
    /**
     * topic
     */
    private String topicName;
    /**
     * 消费组
     */
    private String consumerName;
    /**
     * 数值
     */
    private Double value;
    /**
     * 消息类型 topic consumer
     */
    private String type;

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
