package metamorphosis.workers.sinks;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;
import kafka.utils.TestUtils;
import kafka.utils.VerifiableProperties;
import metamorphosis.kafka.KafkaService;
import metamorphosis.utils.Config;
import metamorphosis.utils.ExponentialBackoffTicker;
import metamorphosis.utils.KafkaUtils;
import metamorphosis.workers.WorkerService;
import net.sf.json.JSONObject;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class WorkerSinkService extends WorkerService<WorkerSink> {

  private Logger _log = Logger.getLogger(WorkerSinkService.class);
  private ConsumerConnector _consumer;
  HashMap<String, ConsumerIterator<String, String>> _topicToIteratorCache = Maps.newHashMap();
  ExponentialBackoffTicker _ticker = new ExponentialBackoffTicker(100);
  
  public WorkerSinkService(String sourceTopic, KafkaService kafkaService) {
    super(sourceTopic, kafkaService, new WorkerSinkFactory());
  }

  @Override
  protected void processMessage(JSONObject poppedMessage) {
    WorkerSink workerSink = _workerFactory.createWorker(poppedMessage);
    String topic = poppedMessage.getString("topic");
    String type = poppedMessage.getJSONObject("sink").getString("type");
    boolean done = false;
    //Do we really want to process this message? Look for the done message in zk.
    _log.debug("Performing zk check...");
    KafkaService kafkaService = Config.singleton().getOrException("kafka.service");
    CuratorFramework client = null;
    try{
      
      client = CuratorFrameworkFactory.builder()
          //.namespace("gmb")
          .retryPolicy(new ExponentialBackoffRetry(1000, 10))
          .connectString(kafkaService.getZKConnectString("gmb"))
          .build();
      client.start();
      
      String bufferTopicPath = "/buffer/" + topic + "/status/done";
      
      if(client.checkExists().forPath(bufferTopicPath) == null){
        // Yes, we do want to process it.
        _log.debug("Done path (" + bufferTopicPath+ ") not found. Processing message: " + poppedMessage);
      }else{
        // Buffer is done being written to.
        // TODO: Maybe the sinks didn't exhaust them... confirm.
        _log.info("Done path (" + bufferTopicPath+ ") found. Stopping sinks. Running one more time to catch tuples we might not have.");
        // If the done path is found, ignore the _numMessagesOn
        done = true;
      }

    }catch(Exception e){
      _log.error("SinkService crashed when trying to check for zk done message");
      _log.error(ExceptionUtils.getStackTrace(e));
    }finally{
      if(client != null){
        client.close();
      }
    }
    _log.debug("Done with zk check...");
    String clientName = topic + "_" + type;

    ConsumerIterator<String, String> sinkTopicIterator;
    if(_topicToIteratorCache.containsKey(clientName)){
      
      _log.debug("Using cached iterator for topic: " + clientName);
      sinkTopicIterator = _topicToIteratorCache.get(clientName);
      // TODO: Maybe the iterator is in a bad state? Confirm before proceeding

    }else{
      Properties props = KafkaUtils.getDefaultProperties(_kafkaService.getZKConnectString("kafka"), clientName);
      props.put("consumer.timeout.ms", Config.singleton().getOrException("kafka.consumer.timeout.ms"));
      _consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
      _log.debug("New consumer created: " + _consumer.hashCode());
      
      Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
      topicCountMap.put(topic, new Integer(1)); // This consumer will only have one thread
      StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());
      
      sinkTopicIterator = _consumer.createMessageStreams(topicCountMap, stringDecoder, stringDecoder).get(topic).get(0).iterator();
      _log.info("Consumer " + clientName + " instantiated");
      _topicToIteratorCache.put(clientName,sinkTopicIterator);
    }

    long sunkTuples = workerSink.sink(sinkTopicIterator, _queueNumber, done);// If done, sinkUntilTimeout.
    if(sunkTuples > 0){
      _log.info("Sunk #" + sunkTuples + " tuples for topic: " + topic);
      try{
        client = CuratorFrameworkFactory.builder()
            .retryPolicy(new ExponentialBackoffRetry(1000, 100))
            .connectString(kafkaService.getZKConnectString("gmb"))
            .build();
        client.start();
        
        String bufferTopicSizePath = "/buffer/" + topic + "/size";

        String bufferTopicSizeLockPath = "/buffer/" + topic + "/size_lock";
        if(client.checkExists().forPath(bufferTopicSizeLockPath) == null){
          client.create().creatingParentsIfNeeded().forPath(bufferTopicSizeLockPath); // Create lock before we try to acquire it.
        }
        
        InterProcessMutex sizeUpdateMutex = new InterProcessMutex(client, bufferTopicSizeLockPath);
        sizeUpdateMutex.acquire();
        try{
          if(client.checkExists().forPath(bufferTopicSizePath) == null){
            // First to write size.
            client.create().creatingParentsIfNeeded().forPath(bufferTopicSizePath, BigInteger.valueOf(sunkTuples).toByteArray());
          }else{
            long currentCount = new BigInteger(client.getData().forPath(bufferTopicSizePath)).longValue();
            currentCount += sunkTuples;
            client.setData().forPath(bufferTopicSizePath, BigInteger.valueOf(currentCount).toByteArray());
          }
        }finally{
          sizeUpdateMutex.release();
        }
      }catch(Exception e){
        _log.error("Couldn't write size to topic");
        _log.error(ExceptionUtils.getStackTrace(e));
      }finally{
        if(client != null){
          client.close();
        }
      }
    }

    if(done){
      _log.info("Shutting down connector for topic: " + topic);
      _topicToIteratorCache.remove(clientName);
      _consumer.shutdown();
    }else{
      // Only increment retry on successful attempt.
      int shardNum = poppedMessage.getJSONObject("sink").getInt("shard_num");
      if(sunkTuples > 0){
       shardNum += 1;
       _log.info("next shard #" + shardNum + ". topic: " + topic);
      }
      
      //streaming sink, so have to increment shardNum and push back to worker queue
      poppedMessage.getJSONObject("sink").element("shard_num", shardNum);
      List<KeyedMessage<Integer, String>> messages = Lists.newArrayList();
      messages.add(new KeyedMessage<Integer,String>(_sourceTopic, poppedMessage.toString()));
      Properties properties = TestUtils.getProducerConfig(Joiner.on(',').join(_kafkaService.getSeedBrokers()), "kafka.producer.DefaultPartitioner");
      Producer<Integer, String> producer = new Producer<Integer,String>(new ProducerConfig(properties));
      producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
      
      producer.close();
    }
  }

  @Override
  protected void processSchlossMessage(JSONObject poppedMessage) {
    throw new NotImplementedException("Unexpected schloss_message for sinks.");
  }
}