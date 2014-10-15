package metamorphosis.workers.sources;

import java.io.File;
import java.util.List;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;
import metamorphosis.kafka.KafkaService;
import metamorphosis.workers.WorkerService;
import net.sf.json.JSONObject;

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
    WorkerSource workerSource = _workerFactory.createWorker(poppedMessage);
    Pair<File,Iterable<String>> messageIteratorPair = workerSource.getMessageIterator();
    Iterable<String> messageIterator = messageIteratorPair.getValue1();
    Properties properties = TestUtils.getProducerConfig(Joiner.on(',').join(_kafkaService.getSeedBrokers()), "kafka.producer.DefaultPartitioner");
    properties.put("compression.codec", "snappy");
    
    Producer<Integer, String> producer = new Producer<Integer,String>(new ProducerConfig(properties));

    // Distribute strategy
    int msgsSent = 0;
    try{
      for( String workerQueueMessage : messageIterator) {
        String topicQueue = workerSource.getTopic();
        //_log.info("Sending message " + workerQueueMessage + " to queue: " + topic);
        List<KeyedMessage<Integer, String>> messages = Lists.newArrayList();
        messages.add(new KeyedMessage<Integer,String>(topicQueue,workerQueueMessage));
        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
        msgsSent++;
      }
      
    }finally{
      File cachedFile = messageIteratorPair.getValue0();
      _log.info("Messages sent: " + msgsSent + " from file:\t" + cachedFile.getAbsolutePath());
      if(cachedFile != null && cachedFile.exists()){
        _log.debug("Deleting cached file: " + cachedFile.getAbsolutePath());
        cachedFile.delete();
      }
      //Create the producer for this distribution
      producer.close();
      
    }
  }
}
