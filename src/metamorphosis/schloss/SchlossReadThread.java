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
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public abstract class SchlossReadThread<T extends SchlossDistributor> implements Callable<Void> {
  protected static final String API_AUTH_TOKEN = "__zilla_web_of_trust__643eb89103d9490fb3cbc98c06f87dea7e6df97e4ab33cee1221f0f0169cae362305879837b841ef5f2ecab1381db72e0259";

  private String[] _workerQueues;
  private String _messageQueue;
  private SchlossFactory<T> _factory;
  private ExponentialBackoffTicker _ticker = new ExponentialBackoffTicker(100);
  private Logger _log = Logger.getLogger(SchlossService.class);
  protected ArrayList<String> _brokers;

  public SchlossReadThread(String messageTopic, String targetWorkerQueues, SchlossFactory<T> factory){
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

    }while(SchlossService.isRunning.get());
    _log.info("Done with the schloss service loop");
    return null;
  }

  public abstract void handleTimeoutTasks();
}