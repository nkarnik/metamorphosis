package metamorphosis.schloss;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import metamorphosis.schloss.sources.SchlossSource;
import metamorphosis.utils.JSONDecoder;
import metamorphosis.utils.KafkaService;
import metamorphosis.utils.KafkaUtils;
import metamorphosis.utils.LocalKafkaService;
import metamorphosis.utils.Utils;

public class SchlossService {


  protected static final long SLEEP_BETWEEN_READS = 30 * 1000;
  private static AtomicBoolean isRunning;
  private Logger _log = Logger.getLogger(SchlossService.class);
  private List<String> _brokers;
  private String _sourceTopic;
  private String _zkConnectString;
  private List<String> _workerQueues;
  private int _queueToPush;
  private KafkaService _kafkaService;

  public SchlossService(String sourceTopic, List<String> brokers, String zkConnectString, List<String> workerQueues) {
    
    _brokers = brokers;
    _sourceTopic = sourceTopic;
    _zkConnectString = zkConnectString;
    _workerQueues = workerQueues;
    _queueToPush = 0;
    
    
    
  }
  
  public SchlossService(String sourceTopic, List<String> brokers, KafkaService kafkaService, String zkConnectString, List<String> workerQueues) {
    
    _brokers = brokers;
    _sourceTopic = sourceTopic;
    _zkConnectString = zkConnectString;
    _workerQueues = workerQueues;
    _queueToPush = 0;
    _kafkaService = kafkaService;
    
    
    
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
        try{
          // Blocking wait on source topic
          while(isRunning.get() && iterator.hasNext()){
            MessageAndMetadata<String, JSONObject> next = iterator.next();
            JSONObject message = next.message();
            SchlossSource schlossSource = SchlossSourceFactory.createSource(message);
            List<String> workerQueueMessages = schlossSource.getWorkerMessages();
            distributeMessagesToQueues(workerQueueMessages);
            
          }
        }catch(ConsumerTimeoutException e){
          _log.info("No messages yet on " + _sourceTopic + ". Sleeping for " + SLEEP_BETWEEN_READS + " seconds");
        }
        
        _log.info("Done with the schloss service loop");
        return null;
      }
    });
  }

  protected void distributeMessagesToQueues(List<String> workerQueueMessages) {
    // Distribute strategy
    
    for( String workerQueueMessage : workerQueueMessages) {
      
      int numQueues = _workerQueues.size();
      String topicQueue = _workerQueues.get(_queueToPush % numQueues);
      _log.info("Sending message " + workerQueueMessage + " to queue: " + topicQueue);
      _kafkaService.sendMessage(topicQueue, workerQueueMessage);
      _queueToPush += 1;
    
    }
   
    
  }

  private void processMessage(JSONObject message) {
    _log.info("Processing message: " + message.toString());
    
    // Based on type, generate the appropriate messages to workers
    
  }
  
  private ConsumerIterator<String, JSONObject> getIterator() {
    String clientName = "temporary_message_reader";
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

  public static void main(String[] args) {
    // Parse CLI args
    // new SchlosService ();
    //start();
    
  }
}
