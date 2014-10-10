package metamorphosis.workers;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import metamorphosis.kafka.KafkaService;
import metamorphosis.utils.JSONDecoder;
import metamorphosis.utils.KafkaUtils;
import metamorphosis.utils.Utils;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

public abstract class WorkerService<T extends Worker> {
  
  
  protected static final long SLEEP_BETWEEN_READS = 30 * 1000;
  private static AtomicBoolean isRunning;
  private Logger _log = Logger.getLogger(WorkerService.class);
  public String _sourceTopic; //includes queue number
  private Future<Void> _pushThread;
  private Future<Void> _popThread;
  private static ExecutorService _executorPool =  Executors.newFixedThreadPool(10); //(5, 10, 1, TimeUnit.HOURS, new SynchronousQueue<Runnable>());
  private RoundRobinByTopicMessageQueue _topicMessageQueue = new RoundRobinByTopicMessageQueue();
  protected KafkaService _kafkaService;
  protected WorkerFactory<T> _workerFactory;
  protected String _queueNumber;

  public WorkerService(String sourceTopic, KafkaService kafkaService, WorkerFactory<T> workerFactory) {
    _kafkaService = kafkaService;
    _sourceTopic = sourceTopic;
    _queueNumber = _sourceTopic.substring(_sourceTopic.length() - 1, _sourceTopic.length());
    _workerFactory = workerFactory;
  }


  public void start() {
    isRunning = new AtomicBoolean(true);
    _log.info("Starting the roundRobin push thread");
    startRoundRobinPushRead();
    _log.info("Starting the roundRobin pop thread");
    startRoundRobinPopThread();
  }
  
  
  public void startRoundRobinPopThread(){
    _popThread = Utils.run(new Callable<Void>(){

      @Override
      public Void call() throws InterruptedException  {
        _log.info("Entering round robin pop thread");
        while(isRunning.get()){
          //Blocking pop
          _log.info("About to pop from round robin...");
          try {
            final JSONObject poppedMessage = _topicMessageQueue.pop();
            if(poppedMessage == null){
              continue; // Happens when the pop is interrupted
            }
            //Using the executorPool's internal q to send in callables
            _log.info("passing to executor: " + poppedMessage.toString());
            
            _executorPool.submit(new Callable<String>(){
              @Override
              public String call() throws Exception {
                _log.info("Processing message: " + poppedMessage.toString());
                processMessage(poppedMessage);
                _log.info("Completed processing message: " + poppedMessage.toString());
                
                return null;
              }
            });
          } catch (TimeoutException e) {
           continue;
          }
        }
        _log.info("Exiting round robin pop thread");

        return null;
      }
    });
  }
  
  protected abstract void processMessage(final JSONObject poppedMessage);
  
  
  public Future<Void> startRoundRobinPushRead() {
    // Start while loop
    _pushThread = Utils.run(new Callable<Void>(){
      @Override
      public Void call() throws Exception {
        _log.info("Entering worker service loop. Waiting on topic: " + _sourceTopic);
        // Create an iterator
        ConsumerIterator<String, JSONObject> iterator = getMessageTopicIterator();
        while(isRunning.get()){
          try{
            _log.info("waiting on kafka topic iterator...");
        
            // Blocking wait on source topic
            while(iterator.hasNext()){
              MessageAndMetadata<String, JSONObject> messageAndMeta = iterator.next();
              // TODO: Change this to use an executorPool
              _topicMessageQueue.push(messageAndMeta);
            }
          }catch(ConsumerTimeoutException e){
            _log.info("No messages yet on " + _sourceTopic + ". Blocking on iterator.hasNext...");
          }
        }
        _log.info("Done with the shard service loop");
        return null;
      }
    });
    return _pushThread;
  }
  

  private ConsumerIterator<String, JSONObject> getMessageTopicIterator() {
    String clientName = "worker_service_consumer_" + _sourceTopic;
    ConsumerConfig consumerConfig = KafkaUtils.createConsumerConfig(_kafkaService.getZKConnectString(), clientName);
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
    try {
      _log.info("Interrupting round robin pop...");
      _topicMessageQueue.interrupt();
      if(_pushThread != null && !_pushThread.isDone()){
        _log.info("Waiting on pushThread termination");
        _pushThread.get();
      }
      if(_popThread != null && !_popThread.isDone()){
        _log.info("Waiting on popThread termination");
        _popThread.get();
      }
      _log.info("Shutting down executor pool");
      _executorPool.shutdown();
      _log.info("Waiting 3 minutes on executor pool termination");
      _executorPool.awaitTermination(3, TimeUnit.MINUTES);
    } catch (InterruptedException | ExecutionException e) {
      _log.error("Stop failed because: ", e);
    }
    
  }

}
