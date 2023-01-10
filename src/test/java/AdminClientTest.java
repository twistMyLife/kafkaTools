import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdminClientTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties p = new Properties();
        // kafka地址，多个地址用逗号分割
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.50.39:31060");
//        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.50.37:9091,192.168.50.37:9092,192.168.50.37:9093");
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,500);
        p.put(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG,500);
        p.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG,500);
        p.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG,500);
        p.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG,500);
//        p.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG,5);

        AdminClient adminClient = AdminClient.create(p);
//        ListTopicsResult listTopicsResult = adminClient.listTopics();
//        Collection<ConsumerGroupListing> consumerGroupListings = adminClient.listConsumerGroups().all().get();
//        Map<String, ConsumerGroupDescription> myGroup3 = adminClient.describeConsumerGroups(Collections.singleton("myGroup3")).all().get();
        NewTopic test = new NewTopic("mytest", 3, (short) 3);
        adminClient.createTopics(Collections.singletonList(test));
//        DeleteTopicsResult test1 = adminClient.deleteTopics(Collections.singletonList("test"));
//        DescribeTopicsResult test1 = adminClient.describeTopics(Collections.singletonList("test"));
//        Map<String, TopicDescription> stringTopicDescriptionMap = test1.all().get();
//        DescribeClusterResult describeClusterResult = adminClient.describeCluster();

//        Collection<Node> nodes = describeClusterResult.nodes().get();
        adminClient.close();

    }
}
