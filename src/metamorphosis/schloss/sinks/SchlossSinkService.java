package metamorphosis.schloss.sinks;

import java.util.List;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;
import metamorphosis.schloss.SchlossReadThread;
import metamorphosis.utils.ExponentialBackoffTicker;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class SchlossSinkService extends SchlossReadThread<SchlossSink>{

  private List<String> _activeSinkTopics = Lists.newArrayList();
  ExponentialBackoffTicker _ticker = new ExponentialBackoffTicker(100);
  Logger _log = Logger.getLogger(SchlossSinkService.class);

  
  public SchlossSinkService(String messageTopic) {
    super(messageTopic, "worker.sink.queues", new SchlossSinkFactory());
  }

  @Override
  public void distributeMessagesToQueues(String[] workerQueues, List<String> workerQueueMessages, String topic) {
    // Write to all topics.
    _log.info("Schloss Sink sending " + workerQueueMessages.size() + " messages to all " + workerQueues.length + " queues" );
    List<KeyedMessage<Integer, String>> messages = Lists.newArrayList();
    for(String workerSinkQ: workerQueues){
      for(String queueMessage : workerQueueMessages){
        messages.add(new KeyedMessage<Integer,String>(workerSinkQ,queueMessage));  
      }
    }
    // Add to active sink topics so size can be updated
    //_log.info("Adding active sink topic: " + topic);
    //_activeSinkTopics.add(topic);
    
    //Create the producer for this distribution
    Properties properties = TestUtils.getProducerConfig(Joiner.on(',').join(_brokers), "kafka.producer.DefaultPartitioner");
    Producer<Integer, String> producer = new Producer<Integer,String>(new ProducerConfig(properties));
    producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
    producer.close(); 
  }

  @Override
  public void handleTimeoutTasks() {
//    if(_activeSinkTopics.size() == 0){
//      if(_ticker.tick()){
//        _log.info("[sampled #" + _ticker.counter() + "] No active topics.");
//      }
//    }else{
//      _log.info("Handling topic size updates: Active topics: " + Joiner.on(",").join(_activeSinkTopics));
//    }
//    // Every timeout, update row count of the active sinks
//    List<String> removals = Lists.newArrayList();
//    KafkaService kafkaService = Config.singleton().getOrException("kafka.service");
//    // Check if sink is inactive
//    for(String topic: _activeSinkTopics){
//      _log.info("Handling API Size update for topic: " + topic);
//
//      if(KafkaUtils.isSinkActive(topic)){
//         removals.add(topic);
//      }
//      // Regardless of removals, update topic size to API.
//      long messageCount = kafkaService.getTopicMessageCount(topic);
//      JSONObject params = new JSONObject();
//      params.put("relation_id", topic);
//      params.put("size", messageCount);
//      _log.info("New topic size: " + topic + ":: " + messageCount);
//      if(messageCount > 0){
//        try {
//          String path = "/relations/" + topic + "/size";
//          _log.debug("Sending message: " + params.toString() + " to path: " + path);
//          RestAPIHelper.post(path, params.toString(), API_AUTH_TOKEN);
//        } catch (APIException e) {
//          _log.error("Failed updating topic size : " + topic);
//          e.printStackTrace();
//          //throw new APIException("Set size failed for relation: " + topic);
//        }
//      }
//      _log.info("Done handling API update for topic: " + topic);
//
//    }
//    _activeSinkTopics.removeAll(removals);
  }
}