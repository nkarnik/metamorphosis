package metamorphosis.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

public class RemoteKafkaService implements KafkaService {

  private EmbeddedZookeeper _zkServer;
  private ArrayList<KafkaServer> _servers;
  private Producer<Integer, String> _producer;
  private static Logger _log = Logger.getLogger(LocalKafkaService.class);

  /**
   * Creates a local Kafka service that initializes Zookeeper and sets up brokers
   * @param numBrokers
   */
  public RemoteKafkaService(int numBrokers) {

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
  /** Send a single message */
  public void sendMessage(String topic, String message) {
    Producer<Integer, String> producer = getSimpleProducer();
    // send message
    List<KeyedMessage<Integer, String>> messages = new ArrayList<>();
    messages.add(new KeyedMessage<Integer, String>(topic, message));
    //_log.info("Sending message to broker: " + message);
    producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
    producer.close();
  }

  /**
   * Send a batch of messages
   * @param topic
   * @param messages
   */
  public void sendMessageBatch(String topic, List<String>messages) {
    Producer<Integer, String> producer = getSimpleProducer();
    List<KeyedMessage<Integer, String>> keyedMessages = new ArrayList<>();

    for (String message : messages ){
      keyedMessages.add(new KeyedMessage<Integer, String>(topic, message));
    }
    producer.send(scala.collection.JavaConversions.asScalaBuffer(keyedMessages));
    producer.close();
  }

}
