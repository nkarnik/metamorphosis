package metamorphosis.workers.sources;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import metamorphosis.MetamorphosisService;
import metamorphosis.kafka.KafkaService;
import metamorphosis.utils.Config;
import metamorphosis.utils.Utils;
import metamorphosis.workers.WorkerService;
import net.sf.json.JSONObject;

import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class WorkerSourceService extends WorkerService<WorkerSource> {

  private Logger _log = Logger.getLogger(WorkerSourceService.class);
  
  public WorkerSourceService(String sourceTopic, KafkaService kafkaService) {
    super(sourceTopic, kafkaService, new WorkerSourceFactory());
  }

  @Override
  protected void processMessage(final JSONObject poppedMessage) {
    
    if(poppedMessage.containsKey("SCHLOSS_DONE")){
      // Special message signifying end of topic
      String topic = poppedMessage.getString("SCHLOSS_DONE");
      _log.info("Processing schloss done message for topic: " + topic);
      // Write to zk that you have in fact completed all such messages
      String gmbZkConnectString = Config.singleton().getOrException("kafka.zookeeper.connect");
      String brokerList = Config.singleton().getOrException("kafka.brokers");
      int numBrokers = brokerList.split(",").length;
      String ourQueue = Config.singleton().getOrException(MetamorphosisService.workerSourceQueue);
      try {
        CuratorFramework client = CuratorFrameworkFactory.newClient(gmbZkConnectString, new ExponentialBackoffRetry(1000, 3));
        String bufferTopicPath = "/buffer/" + topic + "/status";
        if(client.checkExists().forPath(bufferTopicPath) == null){
          client.create().creatingParentsIfNeeded().forPath(bufferTopicPath);
          client.create().creatingParentsIfNeeded().forPath(bufferTopicPath + "/workers");
        }
        InterProcessMutex lock = new InterProcessMutex(client, bufferTopicPath + "/lock");
        if(lock.acquire(5, TimeUnit.SECONDS)){
          _log.info("Lock acquired");
          List<String> workersDone = client.getChildren().forPath(bufferTopicPath + "/workers/");
          if(workersDone == null || workersDone.size() < numBrokers - 1){
            //Not the last worker to finish. write a worker done
            _log.info("Not last, writing to " + bufferTopicPath + "/workers/" + ourQueue);
            client.setData().forPath(bufferTopicPath + "/workers/" + ourQueue, "true".getBytes());
          }
          if(workersDone.size() == numBrokers - 1){
            // Last to finish. Write the path that gmb will read
            String zNodePath = bufferTopicPath + "/done"; 
            client.setData().forPath(bufferTopicPath + "/done", "true".getBytes());
            _log.info("Create znode: " + zNodePath);
          }
        }
        lock.release();
      } catch (Exception e) {
        _log.error("Couldn't process SCHLOSS_DONE message");
        throw new RuntimeException(e);
      }
      // read list of children
      // IF all other brokers are done
      
      // Write the zNodePath
      
      // Release lock

    }
    WorkerSource workerSource = _workerFactory.createWorker(poppedMessage);
    Pair<File,Iterable<String>> messageIteratorPair = workerSource.getMessageIterator();
    Iterable<String> messageIterator = messageIteratorPair.getValue1();
    Properties props = new Properties();
    props.put("metadata.broker.list", Joiner.on(",").join(_kafkaService.getSeedBrokers()));
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
    props.put("partitioner.type", "async");
    props.put("queue.buffering.max.ms", "3000");
    props.put("queue.buffering.max.messages", "200");
    props.put("compression.codec", "snappy");
    props.put("request.required.acks", "1");
    
    Producer<Integer, String> producer = new Producer<Integer,String>(new ProducerConfig(props));

    String topicQueue = workerSource.getTopic();
    // Distribute strategy
    int msgsSent = 0;
    int bytesReceived = 0;
    List<KeyedMessage<Integer, String>> messages = Lists.newArrayList();
    try{
      for( String workerQueueMessage : messageIterator) {
        //_log.info("Sending message " + workerQueueMessage + " to queue: " + topic);
        messages.clear();
        bytesReceived += workerQueueMessage.getBytes().length;
        messages.add(new KeyedMessage<Integer,String>(topicQueue,workerQueueMessage));
        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
        msgsSent++;
      }
      
    }finally{
      File cachedFile = messageIteratorPair.getValue0();
      _log.info("Messages sent: " + msgsSent + ". Bytes: " + bytesReceived + " from file:\t" + cachedFile.getAbsolutePath());
      
      if(cachedFile != null && cachedFile.exists()){
        _log.debug("Deleting cached file: " + cachedFile.getAbsolutePath());
        cachedFile.delete();
      }
      //Create the producer for this distribution
      producer.close();
      
    }
  }
}
