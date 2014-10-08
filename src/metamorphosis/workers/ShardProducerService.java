package metamorphosis.workers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;
import kafka.utils.TestUtils;
import kafka.utils.VerifiableProperties;
import metamorphosis.schloss.SchlossService;
import metamorphosis.schloss.SchlossSourceFactory;
import metamorphosis.schloss.sources.SchlossSource;
import metamorphosis.utils.JSONDecoder;
import metamorphosis.utils.KafkaUtils;
import metamorphosis.utils.Utils;
import metamorphosis.workers.sources.WorkerSource;
import net.sf.json.JSONObject;

public class ShardProducerService {
  
  
  protected static final long SLEEP_BETWEEN_READS = 30 * 1000;
  private static AtomicBoolean isRunning;
  private Logger _log = Logger.getLogger(ShardProducerService.class);
  private List<String> _brokers;
  private String _sourceTopic;
  private String _zkConnectString;
  private List<String> _workerQueues;
  
  

  public ShardProducerService(String sourceTopic, List<String> seedBrokers, String zkConnectString, List<String> workerQueues) {
    _brokers = seedBrokers;
    _zkConnectString = zkConnectString;
    _sourceTopic = sourceTopic;
    _workerQueues = workerQueues;

  }
  
  public void start() {
    // Start while loop
    Future<String> sourceReadThread = Utils.run(new Callable<String>(){
      @Override
      public String call() throws Exception {
        _log.info("Entering schloss service loop");
        isRunning = new AtomicBoolean(true);
        // Create an iterator
        ConsumerIterator<String, JSONObject> iterator = getIterator();
        while(isRunning.get()){
          try{
            // Blocking wait on source topic
            while(iterator.hasNext()){
              MessageAndMetadata<String, JSONObject> next = iterator.next();
              JSONObject message = next.message();
              WorkerSource workerSource = WorkerSourceFactory.createSource(message);
              List<String> workerQueueMessages = workerSource.getMessages();
              distributeMessagesToTopic(workerQueueMessages, workerSource.getTopic());
              
            }
          }catch(ConsumerTimeoutException e){
            _log.info("No messages yet on " + _sourceTopic + ". Blocking on iterator.hasNext...");
          }
        }
        _log.info("Done with the schloss service loop");
        return null;
      }
    });
  }
  
  protected void distributeMessagesToTopic(List<String> workerMessages, String topic) {
    // Distribute strategy
    List<KeyedMessage<Integer, String>> messages = Lists.newArrayList();
    for( String workerQueueMessage : workerMessages) {
      String topicQueue = topic;
      //_log.info("Sending message " + workerQueueMessage + " to queue: " + topic);
      messages.add(new KeyedMessage<Integer,String>(topicQueue,workerQueueMessage));
 
    }
    _log.info("sending message to " + Joiner.on(',').join(_brokers));
    //Create the producer for this distribution
    Properties properties = TestUtils.getProducerConfig(Joiner.on(',').join(_brokers), "kafka.producer.DefaultPartitioner");
    Producer<Integer, String> producer = new Producer<Integer,String>(new ProducerConfig(properties));
    producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
    producer.close(); 
    
  }
  
  private ConsumerIterator<String, JSONObject> getIterator() {
    String clientName = "schloss_service_consumer";
    ConsumerConfig consumerConfig = KafkaUtils.createConsumerConfig(_zkConnectString, clientName);
    ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(_sourceTopic, new Integer(1)); // This consumer will only have one thread
    StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());
    KafkaStream<String,JSONObject> kafkaStream = consumer.createMessageStreams(topicCountMap, stringDecoder, new JSONDecoder()).get(_sourceTopic).get(0);
    ConsumerIterator<String, JSONObject> iterator = kafkaStream.iterator();
    _log.info("Consumer " + clientName + " instantiated");
    return iterator;
  }
  
  public void stop(){
    isRunning.set(false);
  }

}
