package metamorphosis.workers;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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
import metamorphosis.utils.Config;
import metamorphosis.utils.ExponentialBackoffTicker;
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
  private ExecutorService _executorPool =  Executors.newFixedThreadPool(10); //(5, 10, 1, TimeUnit.HOURS, new SynchronousQueue<Runnable>());
  private RoundRobinByTopicMessageQueue _topicMessageQueue; 
  protected KafkaService _kafkaService;
  protected WorkerFactory<T> _workerFactory;
  protected int _queueNumber;
  private ExponentialBackoffTicker _ticker = new ExponentialBackoffTicker(100);
  
  public WorkerService(String sourceTopic, KafkaService kafkaService, WorkerFactory<T> workerFactory) {
    _kafkaService = kafkaService;
    _sourceTopic = sourceTopic;
    String[] split = _sourceTopic.split("_");
    _queueNumber = Integer.parseInt(split[split.length - 1]);
    _workerFactory = workerFactory;
    _topicMessageQueue = new RoundRobinByTopicMessageQueue(_sourceTopic);
  }

  public void setRunning(boolean state) {
    isRunning = new AtomicBoolean(state);
    
  }

  public void start() {
    isRunning = new AtomicBoolean(true);
    _log.info("Starting the roundRobin push thread");
    startRoundRobinPushRead();
    _log.info("Starting the roundRobin pop thread");
    startRoundRobinPopThread();
  }
  
  
  public Future<Void> startRoundRobinPopThread(){
    _popThread = Utils.run(new Callable<Void>(){

      @Override
      public Void call() throws InterruptedException  {
        _log.info("Entering round robin pop thread");
        do{
          //Blocking pop
          try {
            final JSONObject poppedMessage = _topicMessageQueue.pop();
            if(poppedMessage == null){
              continue; // Happens when the pop is interrupted
            }
            //Using the executorPool's internal q to send in callables
            _log.debug(this + " is passing to executor: " + poppedMessage.toString());
            if(_executorPool.isShutdown()){
              _executorPool =  Executors.newFixedThreadPool(2); 
            }
            _executorPool.submit(new Callable<String>(){
              @Override
              public String call() {
                _log.debug("Processing message: " + poppedMessage.toString());
                try{
                  processMessage(poppedMessage);
                }catch(Exception e){
                  _log.error("Process message error!!");
                  e.printStackTrace();
                }
                _log.debug("Completed processing message: " + poppedMessage.toString());
                return null;
              }
            });
          } catch (TimeoutException e) {
            
           continue;
          }
        }while(isRunning.get());
        _log.info("Exiting round robin pop thread");

        return null;
      }
    });
    return _popThread;
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
        do{
          try{
            //_log.debug("waiting on kafka topic iterator...");
        
            // Blocking wait on source topic
            while(iterator.hasNext()){
              MessageAndMetadata<String, JSONObject> messageAndMeta = iterator.next();
              // TODO: Change this to use an executorPool
              _topicMessageQueue.push(messageAndMeta);
            }
          }catch(ConsumerTimeoutException e){
            _log.debug("No messages yet on " + _sourceTopic + ". Blocking on iterator.hasNext...");
          }
        }while(isRunning.get());
        _log.info("Done with the shard service loop");
        return null;
      }
    });
    return _pushThread;
  }
  

  private ConsumerIterator<String, JSONObject> getMessageTopicIterator() {
    String clientName = "worker_service_consumer_" + _sourceTopic;
    Properties props = KafkaUtils.getDefaultProperties(_kafkaService.getZKConnectString("kafka"), clientName);
    String consumerTimeout = Config.singleton().getOrException("kafka.consumer.timeout.ms");
    props.put("consumer.timeout.ms", consumerTimeout);
    ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

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
  
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException{
    _executorPool.shutdown();
    return _executorPool.awaitTermination(timeout, unit);
  }
}
