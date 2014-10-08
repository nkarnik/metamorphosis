package metamorphosis.kafka;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
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
      String brokerHostsString = Config.singleton().getOrException("buffer.kafka.brokers");
      _seedBrokers = Arrays.asList(brokerHostsString.split(","));
    }
    // Get brokers from start_gmb
    return _seedBrokers;
  }

  protected ZkClient createZKClient() {
    String connectString = getZKConnectString();
    return new ZkClient(connectString, 30000, 30000, ZKStringSerializer$.MODULE$);
  }


  public String getZKConnectString() {
    String zookeeperHost = Config.singleton().getOrException("buffer.kafka.zookeeper.host");
    String zookeeperPort = Config.singleton().getOrException("buffer.kafka.zookeeper.port");

    return zookeeperHost + ":" + zookeeperPort + "/kafka";
  }

  public int getNumPartitions(String topic) {
    TopicMetadata metadata = getTopicMetadata(topic);
    return metadata.partitionsMetadata().size();
  }

  public TopicMetadata getTopicMetadata(String topic) {
    ZkClient client = createZKClient();
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
 
  public void createTopic(String topic, int partitions, int replicationFactor){
    // create topic
    ZkClient client = createZKClient();
    AdminUtils.createTopic(client, topic, partitions, replicationFactor, new Properties());
    client.close();
  }
}
