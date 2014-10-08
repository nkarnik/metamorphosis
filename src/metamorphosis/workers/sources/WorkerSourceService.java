package metamorphosis.workers.sources;

import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import metamorphosis.kafka.KafkaService;
import metamorphosis.workers.Worker;
import metamorphosis.workers.WorkerService;
import net.sf.json.JSONObject;

public class WorkerSourceService extends WorkerService {

  private Logger _log = Logger.getLogger(WorkerSourceService.class);
  
  public WorkerSourceService(String sourceTopic, KafkaService kafkaService) {
    super(sourceTopic, kafkaService, new WorkerSourceFactory());
  }

  @Override
  protected void processMessage(final JSONObject poppedMessage) {
    Worker workerSource = _workerFactory.createWorker(poppedMessage);
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
