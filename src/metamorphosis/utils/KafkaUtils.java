package metamorphosis.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.cluster.Broker;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
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
  
  /**
   * 
   * @param topic
   * @param client, must be started
   * @return
   */
  public static  boolean isSinkActive(String topic, CuratorFramework client) {
    boolean done = false;
    _log.debug("Performing zk check...");
    try{
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
    }
    _log.debug("Done with zk check...");
    return done;
  }
  
  public static <O, T extends Decoder<O>> ConsumerIterator<String, O> getIterator(String messageTopic, T decoder, String clientPrefix) {
    String clientName = clientPrefix + messageTopic;
    String zkConnectString = Config.singleton().getOrException("kafka.zookeeper.connect");
    Properties props = KafkaUtils.getDefaultProperties(zkConnectString, clientName);
    props.put("consumer.timeout.ms", Config.singleton().<String>getOrException("kafka.consumer.timeout.ms"));

    ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(messageTopic, new Integer(1)); // This consumer will only have one thread
    StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());
    KafkaStream<String,O> kafkaStream = consumer.createMessageStreams(topicCountMap, stringDecoder, decoder).get(messageTopic).get(0);
    ConsumerIterator<String, O> iterator = kafkaStream.iterator();
    _log.info("Consumer " + clientName + " instantiated with properties: ");
    _log.info("");
    _log.info(props);
    _log.info("");
    return iterator;
  }
}
