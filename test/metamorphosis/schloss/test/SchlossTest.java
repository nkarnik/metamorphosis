package metamorphosis.schloss.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import metamorphosis.kafka.LocalKafkaService;
import metamorphosis.schloss.SchlossService;
import metamorphosis.utils.Config;
import net.sf.json.util.JSONBuilder;
import net.sf.json.util.JSONStringer;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class SchlossTest {

  private static final String PRODUCER_QUEUE_PREFIX = "producer_queue_";
  private static final String SOURCE_TOPIC = "source_queue";
  private static final String SINK_TOPIC = "sink_queue";
  private static final int NUM_BROKERS = 3;
  private Logger _log = Logger.getLogger(SchlossTest.class);
  private LocalKafkaService _localKakfaService;
  private List<String> _workerSourceQueues;
  private List<String> _workerSinkQueues;

  @Before
  public void setup() {
    _workerSourceQueues = Lists.newArrayList();
    _workerSinkQueues = Lists.newArrayList();
    _localKakfaService = new LocalKafkaService(NUM_BROKERS);
    // Create required topics
    _localKakfaService.createTopic(SOURCE_TOPIC, 1, 1);
    for (int i = 0; i < NUM_BROKERS; i++) {
      _workerSourceQueues.add(PRODUCER_QUEUE_PREFIX + i);
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
    Config.singleton().put("schloss.source.queue", SOURCE_TOPIC);
    Config.singleton().put("schloss.sink.queue", SINK_TOPIC);
    Config.singleton().put("kafka.zookeeper.connect", _localKakfaService.getZKConnectString());
    Config.singleton().put("kafka.brokers", Joiner.on(",").join(_localKakfaService.getSeedBrokers()));
    Config.singleton().put("worker.source.queues", Joiner.on(",").join(_workerSourceQueues));
    Config.singleton().put("worker.sink.queues", Joiner.on(",").join(_workerSinkQueues));
    
    SchlossService schlossService = new SchlossService();
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
