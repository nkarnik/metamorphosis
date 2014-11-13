package metamorphosis.schloss.test;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.ExecutionException;

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
  private static final String CONSUMER_TIMEOUT_MS = "1000";
  private Logger _log = Logger.getLogger(SchlossTest.class);
  private LocalKafkaService _localKakfaService;
  private List<String> _workerSourceQueues;
  private List<String> _workerSinkQueues;

  @Before
  public void setup() {
    _workerSourceQueues = Lists.newArrayList();
    _workerSinkQueues = Lists.newArrayList();
    _localKakfaService = new LocalKafkaService(NUM_BROKERS);
    // Create required topicst
    _localKakfaService.createTopic(SOURCE_TOPIC, 1, 1);
    for (int i = 0; i < NUM_BROKERS; i++) {
      _workerSourceQueues.add(PRODUCER_QUEUE_PREFIX + i);
      _localKakfaService.createTopic(PRODUCER_QUEUE_PREFIX + i, 1, 1);
    }

  }

  @Test
  public void testProcessGMBSourceMessage() throws InterruptedException, ExecutionException {

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
    initSchlossServiceConfig();
    
    _localKakfaService.sendMessage(SOURCE_TOPIC, message);
    List<String> readStringMessagesInTopic = _localKakfaService.readStringMessagesInTopic(SOURCE_TOPIC);
    _log.info("Messages in source_queue now are: " + Joiner.on("\n").join(readStringMessagesInTopic));
    
    SchlossService schlossService = new SchlossService();
    // run SchlossService
    schlossService.setRunning(false);
    schlossService.startSourceReadThread()
      .get();

    _log.info("Reading messages for confirmation");
    int receivedMessagesSize = 0;
    for (int i = 0; i < NUM_BROKERS; i++) {
      List<String> messages = _localKakfaService.readStringMessagesInTopic(PRODUCER_QUEUE_PREFIX + i);
      _log.info("There are " + messages.size() + " messages in this queue");
      receivedMessagesSize += messages.size();
    }
    _log.info("Total messages on producer queues: " + receivedMessagesSize);
    // Each broker gets a DONE message.
    assertEquals(10 + NUM_BROKERS, receivedMessagesSize);

  }

  private void initSchlossServiceConfig() {
    // create SchlossService
    Config.singleton().put("schloss.source.queue", SOURCE_TOPIC);
    Config.singleton().put("schloss.sink.queue", SINK_TOPIC);
    Config.singleton().put("kafka.consumer.timeout.ms", CONSUMER_TIMEOUT_MS);
    Config.singleton().put("kafka.zookeeper.connect", _localKakfaService.getZKConnectString() + "/kafka");
    Config.singleton().put("gmb.zookeeper.connect", _localKakfaService.getZKConnectString() + "/gmb");
    Config.singleton().put("kafka.brokers", Joiner.on(",").join(_localKakfaService.getSeedBrokers()));
    Config.singleton().put("worker.source.queues", Joiner.on(",").join(_workerSourceQueues));
    Config.singleton().put("worker.sink.queues", Joiner.on(",").join(_workerSinkQueues));
    Config.singleton().put("kafka.service", _localKakfaService);
  }
}
