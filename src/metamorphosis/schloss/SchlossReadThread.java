package metamorphosis.schloss;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.message.MessageAndMetadata;
import metamorphosis.kafka.KafkaService;
import metamorphosis.utils.Config;
import metamorphosis.utils.ExponentialBackoffTicker;
import metamorphosis.utils.JSONDecoder;
import metamorphosis.utils.KafkaUtils;
import metamorphosis.utils.Utils;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public abstract class SchlossReadThread<T extends SchlossHandler> implements Callable<Void> {
  protected static final String API_AUTH_TOKEN = "__zilla_web_of_trust__643eb89103d9490fb3cbc98c06f87dea7e6df97e4ab33cee1221f0f0169cae362305879837b841ef5f2ecab1381db72e0259";

  private String[] _workerQueues;
  private String _messageQueue;
  private SchlossHandlerFactory<T> _factory;
  private ExponentialBackoffTicker _ticker = new ExponentialBackoffTicker(100);
  private Logger _log = Logger.getLogger(SchlossService.class);
  protected ArrayList<String> _brokers;

  public SchlossReadThread(String messageTopic, String targetWorkerQueues, SchlossHandlerFactory<T> factory){
    String workerQueuesString = Config.singleton().getOrException(targetWorkerQueues);
    _workerQueues = workerQueuesString.split(",");
    _messageQueue = messageTopic;
    _factory = factory;
    _log.info("Created read thread for queue: " + _messageQueue + " with factory of type: " + _factory);
    _brokers = Lists.newArrayList(((String) Config.singleton().getOrException("kafka.brokers")).split(","));

  } 

  public abstract void distributeMessagesToQueues(String[] _workerQueues, List<String> workerQueueMessages, String topic);

  @Override
  public Void call() throws Exception {
    _log.info("Entering schloss service loop for topic: " + _messageQueue);

    // Create an iterator
    ConsumerIterator<String, JSONObject> iterator = KafkaUtils.getIterator(_messageQueue, new JSONDecoder(), "schloss_service_consumer_");
    do{
      try{
        // Blocking wait on source topic
        while(iterator.hasNext()){
          MessageAndMetadata<String, JSONObject> next = iterator.next();
          JSONObject message = next.message();
          if(message == null){
            _log.error("Null object, most likely bad json.");
            continue;
          }
          String topic = null;
          try{
            topic = message.getString("topic");
          }catch(JSONException e){
            _log.error("Bad message, no topic provided!: " + message);
            continue;
          }
          // Schloss is basically a distributor. Each message has a distributor that get's the worker messages
          SchlossHandler schlossHandler = _factory.createSchlossHandler(message);
          if(schlossHandler == null){
            _log.error("Cannot handle message. Ignoring: " + message);
            continue;
          }
          
          _log.info("Processing message: " + message.toString());
          KafkaService kafkaService = Config.singleton().getOrException("kafka.service");
          if(kafkaService.hasTopic(topic)){
              // Do nothing
          }else{
            // Create topic with default settings
            kafkaService.createTopic(topic, 20, 1);
            // _log.info("Waiting 15 seconds for safety");
            // Utils.sleep(15 * 1000); // For safety. Maybe the size check or workers acting immediately after is causing issues?
          }

          List<String> workerQueueMessages = schlossHandler.getWorkerMessages();
          distributeMessagesToQueues(_workerQueues, workerQueueMessages, topic);

        }
      }catch(ConsumerTimeoutException e){
        if(_ticker.tick()){
          _log.info("[sampled #" + _ticker.counter() + "] No messages yet on " + _messageQueue + ". "); 
        }
      }

      // Row counters.

      handleTimeoutTasks();

    }while(SchlossService.isRunning.get()); // Piggyback off of the same boolean, for convenience.
    _log.info("Done with the schloss service loop");
    return null;
  }

  public abstract void handleTimeoutTasks();
}