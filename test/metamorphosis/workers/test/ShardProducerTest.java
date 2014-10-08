package metamorphosis.workers.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import metamorphosis.kafka.LocalKafkaService;
import metamorphosis.schloss.SchlossService;
import metamorphosis.schloss.test.SchlossTest;
import metamorphosis.workers.ShardProducerService;
import net.sf.json.util.JSONBuilder;
import net.sf.json.util.JSONStringer;

import org.apache.log4j.Category;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class ShardProducerTest {
  
  private ArrayList<String> _workerQueues;
  private LocalKafkaService _localKakfaService;
  private String PRODUCER_QUEUE_PREFIX = "producer_queue_";
  private int NUM_BROKERS = 3;
  private Logger _log = Logger.getLogger(ShardProducerTest.class);
  private String DESTINATION_TOPIC = "dest_topic";

  @Before
  public void setup() {
    _workerQueues = new ArrayList<String>();
    
    _localKakfaService = new LocalKafkaService(NUM_BROKERS);
    // Create required topics

    for (int i = 0; i < NUM_BROKERS; i++) {
      
      _workerQueues.add(PRODUCER_QUEUE_PREFIX  + i);
      _localKakfaService.createTopic(PRODUCER_QUEUE_PREFIX + i, 1, 1);
    }
    _localKakfaService.createTopic(DESTINATION_TOPIC, 1, 1);

  }

  @Test
  public void testProcessGMBSourceMessage() throws InterruptedException {

    JSONBuilder builder = new JSONStringer();
    builder.object()
    .key("topic").value(DESTINATION_TOPIC)
    .key("source").object()
        .key("type").value("s3")
        .key("config").object()
          .key("manifest").value("data/homepages/2014/0620/1013632003/part_0002.gz")
          .key("bucket").value("fatty.zillabyte.com")
          .key("credentials").object()
            .key("secret").value("")
            .key("access").value("")
          .endObject()
        .endObject()
      .endObject()
    .endObject();

    String message = builder.toString();

    // GMB sends message to schloss topic
    // create SchlossService
    ShardProducerService shardProducerService = new ShardProducerService(_workerQueues.get(0), 
                                                      _localKakfaService.getSeedBrokers(),
                                                      _localKakfaService.getZKConnectString(), 
                                                      _workerQueues);
    // run SchlossService
    shardProducerService.start();
    // verify that SchlossService fills producer_qs
    _localKakfaService.sendMessage(_workerQueues.get(0), message);
    Thread.sleep(5000);

    _log.info("Reading messages for confirmation");
    List<String> receivedMessages = new ArrayList<String>();
    
    Thread.sleep(5000);
    
    
    _log.info("About to read from topic: " + DESTINATION_TOPIC);
    List<String> messages = _localKakfaService.readStringMessagesInTopic(DESTINATION_TOPIC);
    _log.info("There are " + messages.size() + " messages in this queue");
    receivedMessages.addAll(messages);
    
    _log.info("Total messages on producer queues: " + receivedMessages.size());
    
    assertEquals(695, receivedMessages.size());
    shardProducerService.stop();
  }
  
  

}
