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
import metamorphosis.utils.Utils;
import metamorphosis.workers.WorkerService;
import net.sf.json.JSONObject;

import org.I0Itec.zkclient.ZkClient;
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
    
    if(poppedMessage.containsKey("schloss_message")){
      // Special message signifying end of topic
      String topic = poppedMessage.getString("topic");
      try {
        _log.info("Processing schloss done message for topic: " + topic);
        // Write to zk that you have in fact completed all such messages
        String brokerList = Config.singleton().getOrException("kafka.brokers");
        int numBrokers = brokerList.split(",").length;
        String ourQueue = Config.singleton().getOrException(MetamorphosisService.workerSourceQueue);
        KafkaService kafkaService = Config.singleton().getOrException("kafka.service");
        ZkClient client = kafkaService.createGmbZkClient();
        _log.info("Client connecting ...");
        String bufferTopicPath = "/buffer/" + topic + "/status";
        
        _log.info("Client started: ");
        if(!client.exists(bufferTopicPath)){
          client.createPersistent(bufferTopicPath, true);
          client.createPersistent(bufferTopicPath + "/workers", true);
        }
        String lockPath = bufferTopicPath + "/lock";
        while(client.exists(lockPath)){
          _log.debug("Waiting 500ms to acquire lock for status topic: " + bufferTopicPath);
          Utils.sleep(500);
        }
        client.createPersistent(bufferTopicPath,  "true"); //Lock acquired
        _log.debug("Lock acquired");
        List<String> workersDone = client.getChildren(bufferTopicPath + "/workers");
        if(workersDone == null || workersDone.size() < numBrokers - 1){
          //Not the last worker to finish. write a worker done
          _log.info("Not last, writing to " + bufferTopicPath + "/workers/" + ourQueue);
          client.createPersistent(bufferTopicPath + "/workers/" + ourQueue, "true");
        if(workersDone.size() == numBrokers - 1){
            // Last to finish. Write the path that gmb will read
            String zNodePath = bufferTopicPath + "/done"; 
            client.createPersistent(bufferTopicPath + "/done", "true");
            _log.info("Create znode: " + zNodePath);
          }
        }
        client.delete(lockPath); // Lock released
        _log.debug("Lock released");
      } catch (Exception e) {
        _log.error("Couldn't process schloss_message");
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }else{
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

}
