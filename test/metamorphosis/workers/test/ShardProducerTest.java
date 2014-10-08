package metamorphosis.workers.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import metamorphosis.kafka.LocalKafkaService;
import metamorphosis.schloss.SchlossService;
import metamorphosis.schloss.test.SchlossTest;
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
  private String SOURCE_TOPIC;
  private Logger _log = Logger.getLogger(ShardProducerTest.class);

  @Before
  public void setup() {
    _workerQueues = new ArrayList<String>();
    
    _localKakfaService = new LocalKafkaService(NUM_BROKERS);
    // Create required topics
    for (int i = 0; i < NUM_BROKERS; i++) {
      
      _workerQueues.add(PRODUCER_QUEUE_PREFIX  + i);
      _localKakfaService.createTopic(PRODUCER_QUEUE_PREFIX + i, 1, 1);
    }

  }

  @Test
  public void testProcessGMBSourceMessage() throws InterruptedException {

    JSONBuilder builder = new JSONStringer();
    String destinationTopic = "some_topic";
    builder.object()
    .key("topic").value(destinationTopic)
    .key("source").object()
        .key("type").value("s3")
        .key("config").object()
          .key("manifest").value("data/homepages/20140620.manifest.debug")
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
    SchlossService schlossService = new SchlossService(SOURCE_TOPIC, 
                                                      _localKakfaService.getSeedBrokers(),
                                                      _localKakfaService.getZKConnectString(), 
                                                      _workerQueues);
    // run SchlossService
    schlossService.start();
    // verify that SchlossService fills producer_qs
    _localKakfaService.sendMessage(SOURCE_TOPIC, message);
    Thread.sleep(5000);

    _log.info("Reading messages for confirmation");
    List<String> receivedMessages = new ArrayList<String>();
    for (int i = 0; i < NUM_BROKERS; i++) {
      List<String> messages = _localKakfaService.readStringMessagesInTopic(PRODUCER_QUEUE_PREFIX + i);
      _log.info("There are " + messages.size() + " messages in this queue");
      receivedMessages.addAll(messages);
    }
    _log.info("Total messages on producer queues: " + receivedMessages.size());
    
    assertEquals(10, receivedMessages.size());
    schlossService.stop();
  }
  
  

}
