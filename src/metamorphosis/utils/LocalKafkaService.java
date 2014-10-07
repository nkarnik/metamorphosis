package metamorphosis.utils;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.VerifiableProperties;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * A Local Kafka Service for testing
 *
 */
public class LocalKafkaService implements KafkaService{

  private EmbeddedZookeeper _zkServer;
  private ArrayList<KafkaServer> _servers;
  private Producer<Integer, String> _producer;
  private static Logger _log = Logger.getLogger(LocalKafkaService.class);

  /**
   * Creates a local Kafka service that initializes Zookeeper and sets up brokers
   * @param numBrokers
   */
  public LocalKafkaService(int numBrokers) {

    _log.info("Creating Kafka Service");

    // setup Zookeeper
    String zkConnect = TestZKUtils.zookeeperConnect();
    _zkServer = new EmbeddedZookeeper(zkConnect);

    // setup Brokers
    _servers = new ArrayList<KafkaServer>();
    for (int i = 0; i < numBrokers; i++) {
      Properties brokerConfig = TestUtils.createBrokerConfig(i, TestUtils.choosePort());
      brokerConfig.setProperty("message.max.bytes", "10000000" );
      brokerConfig.setProperty("replica.fetch.max.bytes", "10000000" );
      _servers.add(TestUtils.createServer(new kafka.server.KafkaConfig(brokerConfig), new MockTime()));  
    }
    _log.info("Kafka Service created with " + _servers.size() + " brokers");
    _log.info("\tWith config: " + Joiner.on(";").join(getSeedBrokers()));
    _log.info("Zookeeper for kafka at: " + _zkServer.port());
  }

  
  public String getZKConnectString() {
    return _zkServer.connectString();
  }

  /**
   * Return the list of brokers 
   */
  public List<String> getSeedBrokers(){
    List<String> hosts = new ArrayList<String>();
    for(KafkaServer s : _servers){
      kafka.server.KafkaConfig config = s.config();
      hosts.add(config.hostName() + ":" + config.port());
    }
    return hosts;
  }

  
  /** 
   * Create a new topic on the service 
   */
  public void createTopic(String topic, int partitions, int replicationFactor){
    // create topic
    ZkClient client = createZKClient();
    AdminUtils.createTopic(client, topic, partitions, replicationFactor, new Properties());
    client.close();
    for(int i = 0; i < partitions; i++){
      TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(_servers), topic, i, 5000);  
    }
    _log.info("Topic created: " + topic + " with " + partitions + " partitions and replication: " + replicationFactor);
  }

  protected ZkClient createZKClient() {
    String connectString = getZKConnectString();
    return new ZkClient(connectString, 30000, 30000, ZKStringSerializer$.MODULE$);
  }



  
  /** For mocks only */
  private void initLocalProducer() {
    // setup producer
    Properties properties = TestUtils.getProducerConfig(Joiner.on(',').join(getSeedBrokers()), "kafka.producer.DefaultPartitioner");
    _producer = new Producer<Integer,String>(new ProducerConfig(properties));
  }
  
  /**
   * Builds a simple consumer
   */
  private SimpleConsumer getSimpleConsumer(PartitionMetadata partitionMetadata, int timeout, int bufferSize, String consumerClientName){
    return new SimpleConsumer(partitionMetadata.leader().host(), partitionMetadata.leader().port(), timeout, bufferSize, consumerClientName);
  }
  
  

  /**
   * For configuration and mock use only. 
   */
  public void sendMessage(String topic, String message) {
    if(_producer == null) {
      initLocalProducer();
    }
    // send message
    List<KeyedMessage<Integer, String>> messages = new ArrayList<>();
    messages.add(new KeyedMessage<Integer, String>(topic, message));
    _log.info("Sending message to queue " + topic + " with message: " + message);
    try {
    _producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
    //_producer.close();
    _log.info("Successfully sent message");
    }
    catch (Exception e) {
      _log.info(e.getMessage());
    }
  }
  
  /** Send a batch of messages */
  public void sendMessageBatch(String topic, List<String>messages) {

    if (_producer == null){
      initLocalProducer();
    }
    List<KeyedMessage<Integer, String>> keyedMessages = new ArrayList<>();

    for (String message : messages ){
      keyedMessages.add(new KeyedMessage<Integer, String>(topic, message));
    }
    _producer.send(scala.collection.JavaConversions.asScalaBuffer(keyedMessages));

  }
  
  public List<String> readStringMessagesInTopic(String topic){
    List<String> messages = Lists.newArrayList();
    
    String clientName = "temporary_message_reader";
    ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(KafkaUtils.createConsumerConfig(getZKConnectString(), clientName));

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1)); // This consumer will only have one thread
    StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());
    KafkaStream<String,String> kafkaStream = consumer.createMessageStreams(topicCountMap, stringDecoder, stringDecoder).get(topic).get(0);
    ConsumerIterator<String, String> iterator = kafkaStream.iterator();
    _log.info("Consumer " + clientName + " instantiated");
    try{
      while(iterator.hasNext()){
        messages.add(iterator.next().message());
      }
    }catch(ConsumerTimeoutException e){
      _log.info("Completed reading from " + topic);
    }
    return messages;
  }
  
  /** Returns number of messages found in all partitions for the given topic*/
  public int readAllPartitions(int partitions, String topic, int timeout, int bufferSize, String consumerClientName, int fetchSize) throws UnsupportedEncodingException {
    int numResults = 0;
    for(int partition = 0; partition < partitions; partition++){
      PartitionMetadata partitionMetadata = KafkaUtils.findPartitionMetadata(getSeedBrokers(), topic, partition);
      
      // create a consumer for reading
      SimpleConsumer consumer = getSimpleConsumer(partitionMetadata, timeout, bufferSize, consumerClientName);

      long readOffset = 0;
      int numReads = 0;
      
      // An arbitrary number of reads are required to complete a partition
      while(true){
        FetchRequest req = new FetchRequestBuilder().clientId(consumerClientName)
            .addFetch(topic, partition, readOffset, fetchSize) 
            .build();
        FetchResponse fetchResponse = consumer.fetch(req);
        
        if(fetchResponse.hasError()){
          System.out.println("Error in fetch:: " + fetchResponse.errorCode(topic, partition));
          continue;
        }
        ByteBufferMessageSet messageSet = fetchResponse.messageSet(topic, partition);
        
        // Each fetch request consists of several messages of max size fetchSize.
        for (MessageAndOffset messageAndOffset : messageSet) {
          long currentOffset = messageAndOffset.offset();
          if (currentOffset < readOffset) {
            System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
            continue;
          }
          readOffset = messageAndOffset.nextOffset();
          numResults += 1;
        }

        if(messageSet.sizeInBytes() == 0 || messageSet.sizeInBytes() < fetchSize){
          // Note. this works because we assume that no data is being pushed in while we're consuming,
          // Isn't true always.
          break; // Done with partition
        }
        numReads += 1;
      }
      System.out.println("Partition complete: " + partition + " Reads completed: " + numReads);
      consumer.close();
    }
    return numResults;
  }

  
  
  
  

  public void shutDown() {

    if(_producer != null)
      _producer.close();

    if(_zkServer != null)
      _zkServer.shutdown();

    for(KafkaServer server : _servers){
      if(server!= null)
        server.shutdown();
    }
  }



}
