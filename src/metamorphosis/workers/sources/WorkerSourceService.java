package metamorphosis.workers.sources;

import java.util.List;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;
import metamorphosis.kafka.KafkaService;
import metamorphosis.workers.WorkerService;
import metamorphosis.workers.WorkerSource;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

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
    Iterable<String> messageIterator = workerSource.getMessageIterator();
    produceDataToTopic(messageIterator, workerSource.getTopic());
  }

  /**
   * 
   * @param workerQueueMessages
   * @param topic
   */
  
  protected void produceDataToTopic(Iterable<String> workerQueueMessages, String topic) {
    Properties properties = TestUtils.getProducerConfig(Joiner.on(',').join(_kafkaService.getSeedBrokers()), "kafka.producer.DefaultPartitioner");
    Producer<Integer, String> producer = new Producer<Integer,String>(new ProducerConfig(properties));
    
    
    
    // Distribute strategy
    _log.info("sending messages to " + Joiner.on(',').join(_kafkaService.getSeedBrokers()));
    int msgsSent = 0;
    for( String workerQueueMessage : workerQueueMessages) {
      String topicQueue = topic;
      //_log.info("Sending message " + workerQueueMessage + " to queue: " + topic);
      List<KeyedMessage<Integer, String>> messages = Lists.newArrayList();
      messages.add(new KeyedMessage<Integer,String>(topicQueue,workerQueueMessage));
      producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
      msgsSent++;
    }
    _log.info("messages sent: " + msgsSent);
    //Create the producer for this distribution
    producer.close(); 
  }
}
