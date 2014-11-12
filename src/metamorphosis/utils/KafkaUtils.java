package metamorphosis.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.log4j.Logger;

public class KafkaUtils {

  private static Logger _log = Logger.getLogger(KafkaUtils.class);

  public static Broker findLeader(List<String> seedBrokers, String topic, int partition) {
    return findPartitionMetadata(seedBrokers, topic, partition).leader();
  }
  
  public static PartitionMetadata findPartitionMetadata(List<String> seedBrokers, String topic, int partition) {
    PartitionMetadata returnMetaData = null;
    loop: for (String seed : seedBrokers) {
      SimpleConsumer consumer = null;
      try {
        String[] split = seed.split(":");
        consumer = new SimpleConsumer(split[0], Integer.parseInt(split[1]), 100000, 64 * 1024, "leaderLookup");
        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        TopicMetadataResponse resp = consumer.send(req);

        List<TopicMetadata> metaData = resp.topicsMetadata();
        for (TopicMetadata item : metaData) {
          for (PartitionMetadata part : item.partitionsMetadata()) {
            if (part.partitionId() == partition) {
              returnMetaData = part;
              break loop;
            }
          }
        }
      } catch (Exception e) {
        _log.info("Error communicating with Broker [" + seed + "] to find Leader for [" + topic + ", " + partition + "] Reason: " + e);
      } finally {
        if (consumer != null)
          consumer.close();
      }
    }
    if (returnMetaData == null){
      throw new RuntimeException("No partition (" + partition + ") found for " + topic);
    }
    return returnMetaData;
  }
  
  
  public static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
    Properties props = getDefaultProperties(zookeeper, groupId);
    
    return new ConsumerConfig(props);
  }

  public static Properties getDefaultProperties(String zookeeper, String groupId) {
    Properties props = new Properties();
    props.put("zookeeper.connect", zookeeper);
    props.put("group.id", groupId);
    props.put("fetch.min.bytes", "10000");
    props.put("zookeeper.session.timeout.ms", "4000");
    props.put("zookeeper.sync.time.ms", "2000");
    props.put("auto.commit.interval.ms", "10000");
    props.put("consumer.timeout.ms", "1000");
    props.put("auto.offset.reset", "smallest");
    return props;
  }
  

}
