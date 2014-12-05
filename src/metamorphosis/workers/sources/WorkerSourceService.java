package metamorphosis.workers.sources;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import metamorphosis.MetamorphosisService;
import metamorphosis.kafka.KafkaService;
import metamorphosis.utils.Config;
import metamorphosis.workers.WorkerService;
import net.sf.json.JSONObject;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryOneTime;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class WorkerSourceService extends WorkerService<WorkerSource> {

  private static int MAX_MESSAGE_LENGTH = 100000;
  private Logger _log = Logger.getLogger(WorkerSourceService.class);

  public WorkerSourceService(String sourceTopic, KafkaService kafkaService) {
    super(sourceTopic, kafkaService, new WorkerSourceFactory());
  }

  @Override
  protected void processMessage(final JSONObject poppedMessage) {


    WorkerSource workerSource = _workerFactory.createWorker(poppedMessage);

    Pair<File,Iterable<String>> messageIteratorPair = workerSource.getMessageIterator();
    Iterable<String> messageIterator = messageIteratorPair.getValue1();
    Properties props = new Properties();
    props.put("metadata.broker.list", Joiner.on(",").join(_kafkaService.getSeedBrokers()));
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
    props.put("producer.type", "async");
    props.put("queue.buffering.max.ms", "1000");
    props.put("batch.num.messages","10");
    props.put("queue.buffering.max.messages", "100");
    props.put("compression.codec", "snappy");
    props.put("request.required.acks", "1");

    Producer<Integer, String> producer = new Producer<Integer,String>(new ProducerConfig(props));

    String topicQueue = workerSource.getTopic();
    // Distribute strategy
    int msgsSent = 0;
    int skipped = 0;
    int bytesReceived = 0;
    try{
      
      for( String workerQueueMessage : messageIterator) {
        
        int messageLength = workerQueueMessage.getBytes().length;

        if(messageLength >=  MAX_MESSAGE_LENGTH){
          _log.debug("Tuple too large, skipping: " + workerQueueMessage.substring(0,20) + "...");
          skipped += 1;
          continue;
        }
        //_log.info("Sending message " + workerQueueMessage + " to queue: " + topic);
        List<KeyedMessage<Integer, String>> messages = Lists.newArrayList();
        bytesReceived += messageLength;
        messages.add(new KeyedMessage<Integer,String>(topicQueue,workerQueueMessage));

        try{
          producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
          msgsSent++;
        }catch(kafka.common.FailedToSendMessageException e){
          _log.error("Failed to send. " + e.getMessage());
        }
      }
    }finally{
      File cachedFile = messageIteratorPair.getValue0();
      _log.info("Messages sent: " + msgsSent + ". skipped: " + skipped + " Bytes: " + bytesReceived + " from file: " + cachedFile.getAbsolutePath());

      if(cachedFile != null && cachedFile.exists()){
        _log.debug("Deleting cached file: " + cachedFile.getAbsolutePath());
        cachedFile.delete();
      }
      //Create the producer for this distribution
      producer.close();

    }
  }

  protected void processSchlossMessage(final JSONObject poppedMessage) {
    // Special message signifying end of topic
    String topic = poppedMessage.getString("topic");
    CuratorFramework client = null;
    InterProcessMutex mutex = null;
    try {
      _log.info("Processing schloss done message for topic: " + topic);
      // Write to zk that you have in fact completed all such messages
      String brokerList = Config.singleton().getOrException("kafka.brokers");
      int numBrokers = brokerList.split(",").length;
      String ourQueue = Config.singleton().getOrException(MetamorphosisService.workerSourceQueue);
      KafkaService kafkaService = Config.singleton().getOrException("kafka.service");

      client = CuratorFrameworkFactory.builder()
          //.namespace("gmb")
          .retryPolicy(new ExponentialBackoffRetry(1000, 100))
          .connectString(kafkaService.getZKConnectString("gmb"))
          .build();
      client.start();
      // ZkClient client = kafkaService.createGmbZkClient();
      _log.debug("Client connecting ...");
      String bufferTopicPath = "/buffer/" + topic + "/status";
      String lockPath = bufferTopicPath + "/lock";
      String workersPath = bufferTopicPath + "/workers";

      _log.debug("Client started: ");
      if(client.checkExists().forPath(bufferTopicPath) == null){
        _log.debug("Creating path: " + bufferTopicPath);
        client.create().creatingParentsIfNeeded().forPath(bufferTopicPath);
        client.create().creatingParentsIfNeeded().forPath(workersPath);
        _log.debug("Created path: " + bufferTopicPath);
      }

      mutex = new InterProcessMutex(client, lockPath);

      mutex.acquire();
      _log.debug("Lock acquired");
      try{
        List<String> workersDone = client.getChildren().forPath(workersPath);
        if(workersDone == null || workersDone.size() < numBrokers - 1){
          //Not the last worker to finish. write a worker done
          _log.info("Not last, writing to " + bufferTopicPath + "/workers/" + ourQueue);
          client.create().creatingParentsIfNeeded().forPath(bufferTopicPath + "/workers/" + ourQueue);
          
        }else{
          // Last to finish. Write the path that gmb will read
          String zNodePath = bufferTopicPath + "/done"; 
          client.create().creatingParentsIfNeeded().forPath(zNodePath);
          _log.info("Create znode: " + zNodePath);
        }
      }finally{
        try {
          mutex.release();
        } catch (Exception e) {
          _log.error(ExceptionUtils.getStackTrace(e));
        }
      }
    } catch (Exception e) {
      _log.error("Couldn't process schloss_message");
      _log.error(ExceptionUtils.getStackTrace(e));
    } finally{
      if(client != null){
        client.close();
      }
    }
  }

}
