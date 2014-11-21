package metamorphosis.kafka;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.api.OffsetRequest;
import kafka.api.PartitionMetadata;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.api.TopicMetadata;
import kafka.common.TopicAndPartition;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import metamorphosis.utils.Config;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

public class KafkaService implements Serializable{

  private static final long serialVersionUID = -8053218474650757262L;
  private List<String> _seedBrokers;
  private static Logger _log = Logger.getLogger(KafkaService.class);

  public KafkaService() {
    
  }
  

  public List<String> getSeedBrokers() {
    if(_seedBrokers == null){
      String brokerHostsString = Config.singleton().getOrException("kafka.brokers");
      _seedBrokers = Arrays.asList(brokerHostsString.split(","));
    }
    // Get brokers from start_gmb
    return _seedBrokers;
  }
  
  public ZkClient createKafkaZkClient(){
    return createZKClient("kafka");
  }
  
  public ZkClient createGmbZkClient(){
    return createZKClient("gmb");
  }

  protected ZkClient createZKClient(String namespace) {
    String connectString = getZKConnectString(namespace);
    return new ZkClient(connectString, 30000, 30000, ZKStringSerializer$.MODULE$);
  }
  
  public String getZKConnectString(String namespace) {
    String zookeeperHost = Config.singleton().getOrException("kafka.zookeeper.host");
    String zookeeperPort = Config.singleton().getOrException("kafka.zookeeper.port");


    return zookeeperHost + ":" + zookeeperPort + (namespace == null ? "" : "/" + namespace);    
  }

  public int getNumPartitions(String topic) {
    TopicMetadata metadata = getTopicMetadata(topic);
    return metadata.partitionsMetadata().size();
  }

  public TopicMetadata getTopicMetadata(String topic) {
    ZkClient client = createKafkaZkClient();
    TopicMetadata metadata = AdminUtils.fetchTopicMetadataFromZk(topic, client);
    client.close();
    return metadata;
  }

  public boolean hasTopic(String topic){
    TopicMetadata metadata = getTopicMetadata(topic);
    if(metadata.errorCode() == 3){
      return false;
    }
    return true;
  }

  
  /**
   * For convenience to send to metamorphosis and the tests. Not meant for major tuple transfer
   * Note: Caller must close the producer when done.
   * @return
   */
  protected Producer<Integer,String> getSimpleProducer() {
    // setup producer
    Properties properties = TestUtils.getProducerConfig(Joiner.on(',').join(getSeedBrokers()), "kafka.producer.DefaultPartitioner");
    return new Producer<Integer,String>(new ProducerConfig(properties));
  }
 
  public <K,V> Producer<K,V> getAsyncProducer(String producerName){
  Properties props = new Properties();
    
    props.put("metadata.broker.list", Joiner.on(",").join(_seedBrokers));
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
    props.put("partitioner.type", "async");
    props.put("queue.buffering.max.ms", "3000");
    props.put("queue.buffering.max.messages", "200");
    props.put("request.required.acks", "1");
    
    props.put("client.id", "producer_" + producerName);
    
    ProducerConfig config = new ProducerConfig(props);
    _log.info(producerName + ": Creating kafka producer");
    return new Producer<K,V>(config);
  }
  
  public void createTopic(String topic, int partitions, int replicationFactor){
    // create topic
    ZkClient client = createKafkaZkClient();
    AdminUtils.createTopic(client, topic, partitions, replicationFactor, new Properties());
    client.close();
    _log.info("Topic created: " + topic + " with " + partitions + " partitions and replication: " + replicationFactor);

  }

  /**
   * Creates a topic with 1 partition and 1 replication factor
   * @param queues
   */
  public void ensureQueuesExist(String... queues) {
    for(String queue : queues){
      if(!hasTopic(queue)){
        createTopic(queue, 1, 2);
      }else{
        _log.info("Topic " + queue + " already exists, nothing to do.");
      }
    }
  }
  
  /**
   * Get number of messages in a topic
   * @param topic
   * @return
   */
  public long getTopicMessageCount(String topic){
    
    int timeout = 10000;
    int bufferSize = 10 * 1000 * 1000;
    String clientName = "topicMessageCounter";
    long messageCount = 0;
    TopicMetadata topicMetadata = getTopicMetadata(topic);
    List<PartitionMetadata> partitionsMetadata = scala.collection.JavaConversions.seqAsJavaList(topicMetadata.partitionsMetadata());
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo  = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    int partition = 0;
    
    for(PartitionMetadata partitionMetadata : partitionsMetadata){
      SimpleConsumer consumer = new SimpleConsumer(partitionMetadata.leader().get().host(), partitionMetadata.leader().get().port(), timeout, bufferSize, clientName);
      // Get earliest offset
      requestInfo.clear();
      TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
      requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(OffsetRequest.EarliestTime(), 1));
      kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
      long earlyOffset = consumer.getOffsetsBefore(request).offsets(topic, partition)[0];
      
      // Get latest offset
      requestInfo.clear();
      requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(OffsetRequest.LatestTime(), 1));
      request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
      long latestOffset = consumer.getOffsetsBefore(request).offsets(topic, partition)[0];
      
      
      _log.debug("Partition " + partition + "\t offsets: [" + earlyOffset + " - " + latestOffset + "]");
      messageCount += (latestOffset - earlyOffset);
      partition++;
    }
    return messageCount;
  }

}
