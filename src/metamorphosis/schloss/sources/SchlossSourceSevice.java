package metamorphosis.schloss.sources;

import java.util.List;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;
import metamorphosis.schloss.SchlossReadThread;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class SchlossSourceSevice extends SchlossReadThread<SchlossSource>{

  private Logger _log = Logger.getLogger(SchlossSourceSevice.class);
  private int _queueToPush = 0;
  public SchlossSourceSevice(String messageTopic) {
    super(messageTopic, "worker.source.queues", new SchlossSourceFactory());
    
  }

  public void distributeMessagesToQueues(String[] workerQueues, List<String> workerQueueMessages, String topic) {
    // Distribute strategy
    _log.info("Distributing " + workerQueueMessages.size() + " messages to " + workerQueues.length + " brokers.");
    List<KeyedMessage<Integer, String>> messages = Lists.newArrayList();
    for( String workerQueueMessage : workerQueueMessages) {
      int numQueues = workerQueues.length;
      String workerQueueTopic = workerQueues[_queueToPush % numQueues];

      _log.debug("Distributing message  to queue: " + workerQueueTopic + " msg:: " + workerQueueMessage);
      messages.add(new KeyedMessage<Integer,String>(workerQueueTopic,workerQueueMessage));
      _queueToPush += 1;
    }
    //Create the producer for this distribution
    Properties properties = TestUtils.getProducerConfig(Joiner.on(',').join(_brokers), "kafka.producer.DefaultPartitioner");
    Producer<Integer, String> producer = new Producer<Integer,String>(new ProducerConfig(properties));
    producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
    // Now that messages are distributed, send a DONE message to all the queues for this topic
    messages.clear();
    for( String workerQueue : workerQueues) {
      messages.add(new KeyedMessage<Integer,String>(workerQueue,"{\"topic\":\"" + topic + "\", \"schloss_message\":\"done\"}" ));
    }
    producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
    producer.close();
    _log.info("Done with distribution.");

  }

  @Override
  public void handleTimeoutTasks() {

  }
}