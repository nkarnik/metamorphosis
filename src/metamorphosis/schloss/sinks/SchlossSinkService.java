package metamorphosis.schloss.sinks;

import java.math.BigInteger;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;
import metamorphosis.kafka.KafkaService;
import metamorphosis.schloss.SchlossReadThread;
import metamorphosis.utils.APIException;
import metamorphosis.utils.Config;
import metamorphosis.utils.ExponentialBackoffTicker;
import metamorphosis.utils.KafkaUtils;
import metamorphosis.utils.RestAPIHelper;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
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
    _activeSinkTopics.add(topic);
    
    //Create the producer for this distribution
    Properties properties = TestUtils.getProducerConfig(Joiner.on(',').join(_brokers), "kafka.producer.DefaultPartitioner");
    Producer<Integer, String> producer = new Producer<Integer,String>(new ProducerConfig(properties));
    producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
    producer.close(); 
  }

  @Override
  public void handleTimeoutTasks() {
    Boolean doUpdates = Config.getOrDefault("update_sizes_to_api", false);
    if(!doUpdates){
      // Make it so teamcity doesn't update sizes for testing.
      return;
    }
    String activeSinkTopicsString = Joiner.on(",").join(_activeSinkTopics);
    if(_activeSinkTopics.size() == 0){
      if(_ticker.tick()){
        _log.info("[sampled #" + _ticker.counter() + "] No active topics.");
        return;
      }
    }else{
      _log.info("Handling topic size updates: Active topics: " + activeSinkTopicsString);
    }
    // Every timeout, update row count of the active sinks
    List<String> removals = Lists.newArrayList();
    KafkaService kafkaService = Config.singleton().getOrException("kafka.service");
    // Check if sink is inactive
    JSONArray params = new JSONArray();
    CuratorFramework client = CuratorFrameworkFactory.builder()
        .retryPolicy(new ExponentialBackoffRetry(1000, 10))
        .connectString(kafkaService.getZKConnectString("gmb"))
        .build();
    client.start();
    
    for(String topic: _activeSinkTopics){
      _log.info("Handling API Size update for topic: " + topic);

      if(KafkaUtils.isSinkActive(topic)){
         removals.add(topic);
      }
      // Get count from ZK
      String bufferTopicSizePath = "/buffer/" + topic + "/size";

      String bufferTopicSizeLockPath = "/buffer/" + topic + "/size_lock";
      long messageCount = 0;
      try{
        if(client.checkExists().forPath(bufferTopicSizeLockPath) == null){
          client.create().creatingParentsIfNeeded().forPath(bufferTopicSizeLockPath); // Create lock before we try to acquire it.
        }
        InterProcessMutex sizeUpdateMutex = new InterProcessMutex(client, bufferTopicSizeLockPath);
        if(sizeUpdateMutex.acquire(10, TimeUnit.SECONDS)){
          try{
            if(client.checkExists().forPath(bufferTopicSizePath) == null){
              // No size yet. Ignore.
              continue; // Move to next topic
            }else{
              messageCount = new BigInteger(client.getData().forPath(bufferTopicSizePath)).longValue();
              _log.info("Found messageCount: " + messageCount + " for topic " + topic);
            }
          }finally{
            sizeUpdateMutex.release();
          }
        } 
      }catch(Exception e){
        _log.error("Couldn't get messageCount from zk for topic: " + topic);
      }

      _log.info("New topic size: " + topic + ":: " + messageCount);
      if(messageCount > 0){
        JSONObject topicSize = new JSONObject();
        topicSize.put("relation_id", topic);
        topicSize.put("size", messageCount);
        params.add(topicSize);
      }
    }
    if(params.size() > 0){
      JSONObject sizes = new JSONObject();
      sizes.put("sizes", params);
      try {
        String path = "/relation/sizes_update";
        _log.debug("Sending message: " + sizes.toString() + " to path: " + path);
        RestAPIHelper.post(path, sizes.toString(), API_AUTH_TOKEN);
      } catch (APIException e) {
        _log.error("Failed updating topic size : " + sizes.toString());
        e.printStackTrace();
        //throw new APIException("Set size failed for relation: " + topic);
      }
    }
    _log.info("Done handling API update for topics: " + activeSinkTopicsString);

    if(removals.size() > 0){
      _log.info("Removing topics from active sinks: " + Joiner.on(',').join(removals));
      _activeSinkTopics.removeAll(removals);
    }
  }
}