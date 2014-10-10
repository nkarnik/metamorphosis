package metamorphosis.schloss;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

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
import metamorphosis.schloss.sources.SchlossSource;
import metamorphosis.utils.Config;
import metamorphosis.utils.JSONDecoder;
import metamorphosis.utils.KafkaUtils;
import metamorphosis.utils.Utils;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class SchlossService {

  protected static final long SLEEP_BETWEEN_READS = 30 * 1000;
  private static AtomicBoolean isRunning;
  private Logger _log = Logger.getLogger(SchlossService.class);
  private List<String> _brokers;
  private String _sourceTopic;
  private String _sinkTopic;
  private String _zkConnectString;
  private Future<String> _sourceReadThread;
  private Future<String> _sinkReadThread;
  //private KafkaService _kafkaService;

  
  public SchlossService() {
    
    _brokers = Lists.newArrayList(((String) Config.singleton().getOrException("kafka.brokers")).split(","));
    _zkConnectString = Config.singleton().getOrException("kafka.zookeeper.connect");
    _sourceTopic = Config.singleton().getOrException("schloss.source.queue");
    _sinkTopic = Config.singleton().getOrException("schloss.sink.queue");
  }
  
  

  public void start() {
    // Start while loop
    _sourceReadThread = Utils.run(new SchlossSourceReadThread(_sourceTopic));
    _sinkReadThread = Utils.run(new SchlossSinkReadThread(_sinkTopic));
  }

  
  
  private ConsumerIterator<String, JSONObject> getIterator(String messageTopic) {
    String clientName = "schloss_service_consumer_" + messageTopic;
    ConsumerConfig consumerConfig = KafkaUtils.createConsumerConfig(_zkConnectString, clientName);
    ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(messageTopic, new Integer(1)); // This consumer will only have one thread
    StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());
    KafkaStream<String,JSONObject> kafkaStream = consumer.createMessageStreams(topicCountMap, stringDecoder, new JSONDecoder()).get(messageTopic).get(0);
    ConsumerIterator<String, JSONObject> iterator = kafkaStream.iterator();
    _log.info("Consumer " + clientName + " instantiated");
    return iterator;
  }
  
  public void stop(){
    _log.info("Shutting down schloss");
    isRunning.set(false);
    
    try {
      _log.info("Waiting on source read thread");
      _sourceReadThread.get();
      _log.info("Waiting on sink read thread");
      _sinkReadThread.get();
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }finally{
      _log.info("Shutdown complete");
    }
  }

  
 private abstract class SchlossReadThread implements Callable<String> {
    
    private String[] _workerQueues;
    private String _messageQueue;

    public SchlossReadThread(String messageTopic, String targetWorkerQueues){
      String workerQueuesString = Config.singleton().getOrException(targetWorkerQueues);
      _workerQueues = workerQueuesString.split(",");
      _messageQueue = messageTopic;
      _log.info("Created read thread for queue: " + _messageQueue);
    } 
    
    public abstract void distributeMessagesToQueues(String[] _workerQueues, List<String> workerQueueMessages);
    
    @Override
    public String call() throws Exception {
      _log.info("Entering schloss service loop for topic: " + _messageQueue);
      isRunning = new AtomicBoolean(true);
      // Create an iterator
      ConsumerIterator<String, JSONObject> iterator = getIterator(_messageQueue);
      while(isRunning.get()){
        try{
          // Blocking wait on source topic
          while(iterator.hasNext()){
            MessageAndMetadata<String, JSONObject> next = iterator.next();
            JSONObject message = next.message();
            _log.info("Processing message: " + message.toString());
            SchlossSource schlossSource = SchlossSourceFactory.createSource(message);
            List<String> workerQueueMessages = schlossSource.getWorkerMessages();
            distributeMessagesToQueues(_workerQueues, workerQueueMessages);
            
          }
        }catch(ConsumerTimeoutException e){
          _log.info("No messages yet on " + _sourceTopic + ". Blocking on iterator.hasNext...");
        }
      }
      _log.info("Done with the schloss service loop");
      return null;
    }
  }
 
 public class SchlossSourceReadThread extends SchlossReadThread{

  private int _queueToPush = 0;
  public SchlossSourceReadThread(String messageTopic) {
    super(messageTopic, "worker.source.queues");
  }
  
  public void distributeMessagesToQueues(String[] _workerQueues, List<String> workerQueueMessages) {
    // Distribute strategy
    List<KeyedMessage<Integer, String>> messages = Lists.newArrayList();
    for( String workerQueueMessage : workerQueueMessages) {
      int numQueues = _workerQueues.length;
      String workerQueueTopic = _workerQueues[_queueToPush % numQueues];
      _log.info("Distributing message  to queue: " + workerQueueTopic + " msg:: " + workerQueueMessage);
      messages.add(new KeyedMessage<Integer,String>(workerQueueTopic,workerQueueMessage));
      _queueToPush += 1;
    }
    //Create the producer for this distribution
    Properties properties = TestUtils.getProducerConfig(Joiner.on(',').join(_brokers), "kafka.producer.DefaultPartitioner");
    Producer<Integer, String> producer = new Producer<Integer,String>(new ProducerConfig(properties));
    producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
    producer.close(); 
    
  }
 }
 
 public class SchlossSinkReadThread extends SchlossReadThread{

  public SchlossSinkReadThread(String messageTopic) {
    super(messageTopic, "worker.sink.queues");

  }

  @Override
  public void distributeMessagesToQueues(String[] workerQueues, List<String> workerQueueMessages) {
    // Write to all topics.
    for(String workerSinkQ: workerQueues){
      
    }
    
  }
   
 }
}
