package net.codingw.kafka.learning;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AdminClientTest {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        AdminClient adminClient = AdminClient.create(properties);
        ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets("dw_test_consumer_2021112701");
        System.out.println(result.partitionsToOffsetAndMetadata().get());

//        List<String> consumerGroups = new ArrayList<>();
//        consumerGroups.add("dw_test_consumer_2021112701");
//
//        DescribeConsumerGroupsResult result1 = adminClient.describeConsumerGroups(consumerGroups);
//
//
//        System.out.println(result1.describedGroups().values());


    }
}
