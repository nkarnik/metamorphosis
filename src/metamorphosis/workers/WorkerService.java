package metamorphosis.workers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
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
import metamorphosis.utils.JSONDecoder;
import metamorphosis.utils.KafkaUtils;
import metamorphosis.utils.Utils;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

public abstract class WorkerService<T extends Worker> {
  
  
  private static AtomicBoolean isRunning;
  private Logger _log = Logger.getLogger(WorkerService.class);
  public String _sourceTopic; //includes queue number
  private Future<Void> _pushThread;
  private Future<Void> _popThread;
  private ExecutorService _executorPool;
  private RoundRobinByTopicMessageQueue _topicMessageQueue; 
  protected KafkaService _kafkaService;
  protected WorkerFactory<T> _workerFactory;
  protected int _queueNumber;
  private ConcurrentHashMap<String, List<Future<Boolean>>> _perTopicFutures = new ConcurrentHashMap<>();
  
  public WorkerService(String sourceTopic, KafkaService kafkaService, WorkerFactory<T> workerFactory) {
    _kafkaService = kafkaService;
    _sourceTopic = sourceTopic;
    String[] split = _sourceTopic.split("_");
    _queueNumber = Integer.parseInt(split[split.length - 1]);
    _workerFactory = workerFactory;
    _topicMessageQueue = new RoundRobinByTopicMessageQueue(_sourceTopic);
    initThreadPool();
  }

  private void initThreadPool() {
    
    _executorPool =  Executors.newFixedThreadPool(10);
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
              initThreadPool(); 
            }
            
            String topic = poppedMessage.getString("topic");
            if(!_perTopicFutures.containsKey(topic)){
              _log.info("Adding new futures list for topic: " + topic);
              _perTopicFutures.put(topic, new ArrayList<Future<Boolean>>());
            }
            
            List<Future<Boolean>> topicFutures = _perTopicFutures.get(topic);
            // Ensure that our futures are clean. Any completed futures should be removed.
            Iterator<Future<Boolean>> iterator = topicFutures.iterator();
            _log.info("Futures before cleanup: " + topicFutures.size());
            while(iterator.hasNext()){
              Future<Boolean> next = iterator.next();
              if(next.isDone() || next.isCancelled()){
                iterator.remove();
              }
            }
            _log.info("Futures after cleanup: " + topicFutures.size());
            if(poppedMessage.containsKey("schloss_message")){
              // Wait on all currently running worker messages for this topic.
              _log.info("Processing a schloss_message for topic: " + topic + ". Awaiting " + topicFutures.size() + " futures.");
              
              for(Future<Boolean> future : topicFutures){
                future.get(); // We don't really care about the output. We just need to know that it is done.
              }
              _log.info("Done waiting on futures. Finally processing the schloss message... ");
              processSchlossMessage(poppedMessage);
              
            }else{ // Not a schloss message
              Future<Boolean> topicFuture = _executorPool.submit(new Callable<Boolean>(){
                @Override
                public Boolean call() {
                  _log.debug("Processing message: " + poppedMessage.toString());
                  try{
                    processMessage(poppedMessage);
                  }catch(Exception e){
                    _log.error("Process message error!! ");
                    e.printStackTrace();
                    return false;
                  }
                  _log.debug("Completed processing message: " + poppedMessage.toString());
                  return true;
                }
              });
              topicFutures.add(topicFuture);  
              _log.info("Added a future. Total futures now :: " + topicFutures.size());
            }
            

          } catch (TimeoutException | ExecutionException e) {
           _log.error("Timeout in WorkerService: " + e.getMessage());
           continue;
          }
        }while(isRunning.get());
        _log.info("Exiting round robin pop thread");

        return null;
      }
    });
    return _popThread;
  }
  
  protected abstract void processSchlossMessage(JSONObject poppedMessage);

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
    props.put("consumer.timeout.ms", Config.singleton().<String>getOrException("kafka.consumer.timeout.ms"));
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
