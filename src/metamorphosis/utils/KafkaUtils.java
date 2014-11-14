package metamorphosis.utils;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import kafka.cluster.Broker;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import metamorphosis.kafka.KafkaService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
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
  
  public static  boolean isSinkActive(String topic) {
    boolean done = false;
    _log.debug("Performing zk check...");
    KafkaService kafkaService = Config.singleton().getOrException("kafka.service");
    CuratorFramework client = null;
    try{
      
      client = CuratorFrameworkFactory.builder()
          //.namespace("gmb")
          .retryPolicy(new ExponentialBackoffRetry(1000, 5))
          .connectString(kafkaService.getZKConnectString("gmb"))
          .build();
      client.start();
      
      String bufferTopicPath = "/buffer/" + topic + "/status/done";
      
      if(client.checkExists().forPath(bufferTopicPath) == null){
        // Yes, we do want to process it.
        _log.debug("Done path (" + bufferTopicPath+ ") not found." );
      }else{
        // Buffer is done being written to.
        // TODO: Maybe the sinks didn't exhaust them... confirm.
        _log.info("Done path (" + bufferTopicPath+ ") found.");
        done = true;
      }

    }catch(Exception e){
      _log.error("SinkService crashed when trying to check for zk done message");
      e.printStackTrace();
      throw new RuntimeException(e);
    }finally{
      if(client != null){
        client.close();
      }
    }
    _log.debug("Done with zk check...");
    return done;
  }
}
