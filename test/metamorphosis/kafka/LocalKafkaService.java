package metamorphosis.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.VerifiableProperties;
import kafka.utils.ZKStringSerializer$;
import metamorphosis.utils.KafkaUtils;

import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * A Local Kafka Service for testing
 *
 */
public class LocalKafkaService extends KafkaService{

  private static final long serialVersionUID = 4341775956497299044L;
  private TestingServer _zkServer;
  private ArrayList<KafkaServer> _servers;
  private Producer<Integer, String> _producer;
  private static Logger _log = Logger.getLogger(LocalKafkaService.class);
  
  /**
   * Creates a local Kafka service that initializes Zookeeper and sets up brokers
   * @param numBrokers
   * @throws Exception 
   */
  public LocalKafkaService(int numBrokers) {

    _log.info("Creating Kafka Service");

    // setup Zookeeper
    try {
      _zkServer = new TestingServer(TestUtils.choosePort());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    ZkClient zkClient = new ZkClient(_zkServer.getConnectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
    zkClient.waitUntilConnected();
    zkClient.createPersistent("/kafka");
    zkClient.createPersistent("/gmb");
    zkClient.close();
    
    // setup Brokers
    _servers = new ArrayList<KafkaServer>();
    for (int i = 0; i < numBrokers; i++) {
      Properties brokerConfig = TestUtils.createBrokerConfig(i, TestUtils.choosePort());
      brokerConfig.setProperty("zookeeper.connect", _zkServer.getConnectString() + "/kafka");
      brokerConfig.setProperty("message.max.bytes", "10000000" );
      brokerConfig.setProperty("replica.fetch.max.bytes", "30000000" );
      _log.info(brokerConfig);
      _servers.add(TestUtils.createServer(new kafka.server.KafkaConfig(brokerConfig), new MockTime()));  
    }
    _log.info("Kafka Service created with " + _servers.size() + " brokers");
    _log.info("\tWith config: " + Joiner.on(";").join(getSeedBrokers()));
    _log.info("Zookeeper for kafka at: " + _zkServer.getPort());
  }

  
  public String getZKConnectString() {
    return _zkServer.getConnectString();
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
  @Override
  public void createTopic(String topic, int partitions, int replicationFactor){
    // create topic
    super.createTopic(topic, partitions, replicationFactor);
    // Test can wait until the metadata is appropriately propagated through the cluster
    for(int i = 0; i < partitions; i++){
      TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(_servers), topic, i, 5000);  
    }
    _log.info("New topic's metadata is propagated");
  }

  @Override
  public String getZKConnectString(String namespace) {
    return _zkServer.getConnectString() + "/" + namespace;    
  }
  
  /** For mocks only */
  private void initLocalProducer() {
    // setup producer
    Properties properties = TestUtils.getProducerConfig(Joiner.on(',').join(getSeedBrokers()), "kafka.producer.DefaultPartitioner");
    _producer = new Producer<Integer,String>(new ProducerConfig(properties));
  }

  

  /**
   * For configuration and mock use only. 
   */
  public void sendMessage(String topic, String message) {

    initLocalProducer();
    
    // send message
    List<KeyedMessage<Integer, String>> messages = new ArrayList<>();
    messages.add(new KeyedMessage<Integer, String>(topic, message));
    _log.info("Sending message to queue " + topic + " with message: " + message);
    try {
    _producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
    _producer.close();
    _log.debug("Successfully sent message");
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
    
    String clientName = "temporary_message_reader_" + Math.random();
    ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(KafkaUtils.createConsumerConfig(getZKConnectString("kafka"), clientName));

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1)); // This consumer will only have one thread
    StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());
    KafkaStream<String,String> kafkaStream = consumer.createMessageStreams(topicCountMap, stringDecoder, stringDecoder).get(topic).get(0);
    ConsumerIterator<String, String> iterator = kafkaStream.iterator();
    _log.info("Consumer " + clientName + " instantiated");
    try{
      while(iterator.hasNext()){
        String message = iterator.next().message();
        //_log.info("Next message is " + message.substring(0, 30) + "...");
        messages.add(message);
      }
    }catch(ConsumerTimeoutException e){
      _log.info("Completed reading from " + topic);
    }catch(Exception e) {
      _log.info("Error is: " + e.getMessage());
    }finally{
      _log.info("Done trying to read messages");
    }
    consumer.shutdown();
    return messages;
  }

  public void shutDown() {

    if(_producer != null)
      _producer.close();

    if(_zkServer != null){
      try {
        _zkServer.stop();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    for(KafkaServer server : _servers){
      if(server!= null)
        server.shutdown();
    }
  }



}
