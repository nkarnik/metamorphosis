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
import metamorphosis.kafka.KafkaService;
import metamorphosis.schloss.sinks.SchlossSink;
import metamorphosis.schloss.sinks.SchlossSinkFactory;
import metamorphosis.schloss.sources.SchlossSource;
import metamorphosis.schloss.sources.SchlossSourceFactory;
import metamorphosis.utils.APIException;
import metamorphosis.utils.Config;
import metamorphosis.utils.ExponentialBackoffTicker;
import metamorphosis.utils.JSONDecoder;
import metamorphosis.utils.KafkaUtils;
import metamorphosis.utils.RestAPIHelper;
import metamorphosis.utils.Utils;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class SchlossService {
  protected static final String API_AUTH_TOKEN = "__zilla_web_of_trust__643eb89103d9490fb3cbc98c06f87dea7e6df97e4ab33cee1221f0f0169cae362305879837b841ef5f2ecab1381db72e0259";

  protected static final long SLEEP_BETWEEN_READS = 30 * 1000;
  private static AtomicBoolean isRunning;
  private Logger _log = Logger.getLogger(SchlossService.class);
  private List<String> _brokers;
  private String _sourceTopic;
  private String _sinkTopic;
  private String _zkConnectString;
  private Future<Void> _sourceReadThread;
  private Future<Void> _sinkReadThread;
  //private KafkaService _kafkaService;
  ExponentialBackoffTicker _ticker = new ExponentialBackoffTicker(10);

  public SchlossService() {

    _brokers = Lists.newArrayList(((String) Config.singleton().getOrException("kafka.brokers")).split(","));
    _zkConnectString = Config.singleton().getOrException("kafka.zookeeper.connect");
    _sourceTopic = Config.singleton().getOrException("schloss.source.queue");
    _sinkTopic = Config.singleton().getOrException("schloss.sink.queue");

  }



  public void start() {
    // Start while loop
    isRunning = new AtomicBoolean(true);
    startSourceReadThread();
    startSinkReadThread();
  }



  public Future<Void> startSinkReadThread() {
    _sinkReadThread = Utils.run(new SchlossSinkReadThread(_sinkTopic));
    return _sinkReadThread;
  }



  public Future<Void> startSourceReadThread() {
    _sourceReadThread = Utils.run(new SchlossSourceReadThread(_sourceTopic));
    return _sourceReadThread;
  }



  private ConsumerIterator<String, JSONObject> getIterator(String messageTopic) {
    String clientName = "schloss_service_consumer_" + messageTopic;
    Properties props = KafkaUtils.getDefaultProperties(_zkConnectString, clientName);
    props.put("consumer.timeout.ms", Config.singleton().<String>getOrException("kafka.consumer.timeout.ms"));

    ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(messageTopic, new Integer(1)); // This consumer will only have one thread
    StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());
    KafkaStream<String,JSONObject> kafkaStream = consumer.createMessageStreams(topicCountMap, stringDecoder, new JSONDecoder()).get(messageTopic).get(0);
    ConsumerIterator<String, JSONObject> iterator = kafkaStream.iterator();
    _log.info("Consumer " + clientName + " instantiated with properties: ");
    _log.info("");
    _log.info(props);
    _log.info("");
    return iterator;
  }

  public void setRunning(boolean state){
    isRunning = new AtomicBoolean(state);
  }
  public void stop(){
    _log.info("Shutting down schloss");
    isRunning.set(false);

    try {
      if(_sourceReadThread != null && !_sourceReadThread.isDone()){
        _log.info("Waiting on source read thread");
        _sourceReadThread.get();
      }
      if(_sinkReadThread != null && !_sinkReadThread.isDone()){
        _log.info("Waiting on sink read thread");
        _sinkReadThread.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }finally{
      _log.info("Shutdown complete");
    }
  }


  private abstract class SchlossReadThread<T extends SchlossDistributor> implements Callable<Void> {

    private String[] _workerQueues;
    private String _messageQueue;
    private SchlossFactory<T> _factory;

    public SchlossReadThread(String messageTopic, String targetWorkerQueues, SchlossFactory<T> factory){
      String workerQueuesString = Config.singleton().getOrException(targetWorkerQueues);
      _workerQueues = workerQueuesString.split(",");
      _messageQueue = messageTopic;
      _factory = factory;
      _log.info("Created read thread for queue: " + _messageQueue + " with factory of type: " + _factory);
    } 

    public abstract void distributeMessagesToQueues(String[] _workerQueues, List<String> workerQueueMessages, String topic);

    @Override
    public Void call() throws Exception {
      _log.info("Entering schloss service loop for topic: " + _messageQueue);

      // Create an iterator
      ConsumerIterator<String, JSONObject> iterator = getIterator(_messageQueue);
      do{
        try{
          // Blocking wait on source topic
          while(iterator.hasNext()){
            MessageAndMetadata<String, JSONObject> next = iterator.next();
            JSONObject message = next.message();
            String topic = message.getString("topic");

            _log.info("Processing message: " + message.toString());
            KafkaService kafkaService = Config.singleton().getOrException("kafka.service");
            if(kafkaService.hasTopic(topic)){
              // Do nothing
            }else{
              // Create topic with default settings
              kafkaService.createTopic(topic, 20, 1); 
            }
            SchlossDistributor schlossHandler = _factory.createSchlossDistributor(message);
            List<String> workerQueueMessages = schlossHandler.getWorkerMessages();
            distributeMessagesToQueues(_workerQueues, workerQueueMessages, topic);

          }
        }catch(ConsumerTimeoutException e){
          if(_ticker.tick()){
            _log.info("[sampled #" + _ticker.counter() + "] No messages yet on " + _messageQueue + ". "); 
          }
        }

        // Row counters.
        // KafkaUtils.readAllPartitions(brokerList, topic, partitions);

        handleTimeoutTasks();

      }while(isRunning.get());
      _log.info("Done with the schloss service loop");
      return null;
    }

    public abstract void handleTimeoutTasks();
  }

  public class SchlossSourceReadThread extends SchlossReadThread<SchlossSource>{

    private int _queueToPush = 0;
    public SchlossSourceReadThread(String messageTopic) {
      super(messageTopic, "worker.source.queues", new SchlossSourceFactory());
    }

    public void distributeMessagesToQueues(String[] workerQueues, List<String> workerQueueMessages, String topic) {
      // Distribute strategy
      _log.info("Distributing " + workerQueueMessages.size() + " messages to " + workerQueues.length + " brokers.");
      List<KeyedMessage<Integer, String>> messages = Lists.newArrayList();
      for( String workerQueueMessage : workerQueueMessages) {
        int numQueues = workerQueues.length;
        String workerQueueTopic = workerQueues[_queueToPush % numQueues];

        _log.debug("Distributing message  to queue: " + workerQueueTopic + " msg:: " + workerQueueMessage);
        messages.add(new KeyedMessage<Integer,String>(workerQueueTopic,workerQueueMessage));
        _queueToPush += 1;
      }
      //Create the producer for this distribution
      Properties properties = TestUtils.getProducerConfig(Joiner.on(',').join(_brokers), "kafka.producer.DefaultPartitioner");
      Producer<Integer, String> producer = new Producer<Integer,String>(new ProducerConfig(properties));
      producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
      // Now that messages are distributed, send a DONE message to all the queues for this topic
      messages.clear();
      for( String workerQueue : workerQueues) {
        messages.add(new KeyedMessage<Integer,String>(workerQueue,"{\"topic\":\"" + topic + "\", \"schloss_message\":\"done\"}" ));
      }
      producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
      producer.close();
      _log.info("Done with distribution.");

    }

    @Override
    public void handleTimeoutTasks() {

    }
  }

  public class SchlossSinkReadThread extends SchlossReadThread<SchlossSink>{

    private List<String> _activeSinkTopics = Lists.newArrayList();

    public SchlossSinkReadThread(String messageTopic) {
      super(messageTopic, "worker.sink.queues", new SchlossSinkFactory());

    }

    @Override
    public void distributeMessagesToQueues(String[] workerQueues, List<String> workerQueueMessages, String topic) {
      // Write to all topics.
      _log.info("Schloss Sink distributing " + workerQueueMessages.size() + " messages to " + workerQueues.length + " queues" );
      List<KeyedMessage<Integer, String>> messages = Lists.newArrayList();
      for(String workerSinkQ: workerQueues){
        for(String queueMessage : workerQueueMessages){
          messages.add(new KeyedMessage<Integer,String>(workerSinkQ,queueMessage));  
        }
      }
      // Add to active sink topics so size can be updated
      _activeSinkTopics.add(topic);
      
      //Create the producer for this distribution
      Properties properties = TestUtils.getProducerConfig(Joiner.on(',').join(_brokers), "kafka.producer.DefaultPartitioner");
      Producer<Integer, String> producer = new Producer<Integer,String>(new ProducerConfig(properties));
      producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
      producer.close(); 
    }

    @Override
    public void handleTimeoutTasks() {
      _log.info("Handling API Size update");
      // Every timeout, update row count of the active sinks
      List<String> removals = Lists.newArrayList();
      KafkaService kafkaService = Config.singleton().getOrException("kafka.service");
      // Check if sink is inactive
      for(String topic: _activeSinkTopics){
        if(KafkaUtils.isSinkActive(topic)){
           removals.add(topic);
        }
        // Regardless of removals, update topic size to API.
        long messageCount = kafkaService.getTopicMessageCount(topic);
        JSONObject params = new JSONObject();
        params.put("relation_id", topic);
        params.put("size", messageCount);
        
        try {
          RestAPIHelper.post("/relations/" + topic + "/size", params.toString(), API_AUTH_TOKEN);
        } catch (APIException e) {
          throw new APIException("Set size failed for relation: " + topic);
        }
      }
    }
  }
}
