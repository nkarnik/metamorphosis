package metamorphosis.schloss.test;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.ExecutionException;

import metamorphosis.kafka.LocalKafkaService;
import metamorphosis.schloss.SchlossService;
import metamorphosis.utils.Config;
import net.sf.json.util.JSONBuilder;
import net.sf.json.util.JSONStringer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class SchlossTest {

  private static final String SCHLOSS_SOURCE_QUEUE = "source_queue";
  private static final String SCHLOSS_SINK_QUEUE = "sink_queue";
  private static final String PRODUCER_QUEUE_PREFIX = "worker_source_queue_";
  private static final String CONSUMER_QUEUE_PREFIX = "worker_sink_queue_";
  private static final int NUM_BROKERS = 3;
  private static final String CONSUMER_TIMEOUT_MS = "1000";
  private Logger _log = Logger.getLogger(SchlossTest.class);
  private LocalKafkaService _localKakfaService;
  private List<String> _workerSourceQueues;
  private List<String> _workerSinkQueues;
  private SchlossService _schlossService;

  @Before
  public void setup() {
    new Config();
    _localKakfaService = new LocalKafkaService(NUM_BROKERS);
    initTopics();
    initSchlossServiceConfig();
    startServices();
  }

  private void startServices() {
    _schlossService = new SchlossService();
    
  }
  

  private void initSchlossServiceConfig() {
    // create SchlossService
    Config.singleton().put("kafka.service", _localKakfaService);
    Config.singleton().put("kafka.consumer.timeout.ms", "1000");
    Config.singleton().put("kafka.zookeeper.connect", _localKakfaService.getZKConnectString() + "/kafka");
    Config.singleton().put("gmb.zookeeper.connect", _localKakfaService.getZKConnectString() + "/gmb");
    Config.singleton().put("kafka.brokers", Joiner.on(",").join(_localKakfaService.getSeedBrokers()));
    Config.singleton().put("schloss.source.queue", SCHLOSS_SOURCE_QUEUE);
    Config.singleton().put("schloss.sink.queue", SCHLOSS_SINK_QUEUE);
    Config.singleton().put("worker.source.queues", Joiner.on(",").join(_workerSourceQueues));
    Config.singleton().put("worker.sink.queues", Joiner.on(",").join(_workerSinkQueues));

    Config.singleton().put("worker.source.queue", _workerSourceQueues.get(0));
    Config.singleton().put("worker.sink.queue", _workerSinkQueues.get(0));
  
  }


  private void initTopics() {
    _workerSourceQueues = Lists.newArrayList();
    _workerSinkQueues = Lists.newArrayList();
    // Create required topics
    _localKakfaService.createTopic(SCHLOSS_SOURCE_QUEUE, 1, 1);
    _localKakfaService.createTopic(SCHLOSS_SINK_QUEUE, 1, 1);

    for (int i = 0; i < NUM_BROKERS; i++) {
      _workerSourceQueues.add(PRODUCER_QUEUE_PREFIX + i);
      _workerSinkQueues.add(CONSUMER_QUEUE_PREFIX + i);
      _localKakfaService.createTopic(PRODUCER_QUEUE_PREFIX + i, 1, 1);
      _localKakfaService.createTopic(CONSUMER_QUEUE_PREFIX + i, 1, 1);
    }
  }

  

  @Test(timeout=1000*200)
  public void testProcessGMBSourceMessage() throws InterruptedException, ExecutionException {
    String destinationTopic = "some_topic";
    sendSourceMessage(destinationTopic);
    
    
    // run SchlossService
    _schlossService.setRunning(false);
    _schlossService.startSourceReadThread()
      .get();

    _log.info("Reading messages for confirmation");
    int receivedMessagesSize = 0;
    for (int i = 0; i < NUM_BROKERS; i++) {
      long numMessages = _localKakfaService.getTopicMessageCount(PRODUCER_QUEUE_PREFIX + i);
      _log.info("There are " + numMessages + " messages in this queue");
      receivedMessagesSize += numMessages;
    }
    _log.info("Total messages on producer queues: " + receivedMessagesSize);
    // Each broker gets a DONE message.
    assertEquals(10 + NUM_BROKERS, receivedMessagesSize);
  }
  
  @Test 
  //@Ignore("Requires local API to be running. TODO: Use MockAPI")
  public void testAPITopicSizeUpdate() throws Exception{
    Config.singleton().put("update_sizes_to_api", true);
    Config.singleton().put("api.port", 5000);
    // Write the size to ZK
    CuratorFramework client = CuratorFrameworkFactory.builder()
        .retryPolicy(new ExponentialBackoffRetry(1000, 10))
        .connectString(_localKakfaService.getZKConnectString("gmb"))
        .build();
    client.start();
    String sinkTopic = "r000001__homepages_v1";
    String bufferTopicSizePath = "/buffer/" + sinkTopic + "/size";

    long numMessages = 101;
    client.create().creatingParentsIfNeeded().forPath(bufferTopicSizePath, BigInteger.valueOf(numMessages).toByteArray());

    _log.info("Sending sink message");

    sendSinkMessage(sinkTopic);
    // Output looks something like this.
//   (RestAPIHelper.java:83) - post: http://localhost:5000/relations/r000001__homepages_v1/size body: {"relation_id":"r000001__homepages_v1","size":100}
//   (RestAPIHelper.java:127) - post returned: {"body":"Size updated successfully."}
    // run SchlossService
    _schlossService.setRunning(false);
    _schlossService.startSinkReadThread()
      .get();

  }

  private void sendSourceMessage(String destinationTopic) {
    JSONBuilder builder = new JSONStringer();
    builder.object()
    .key("topic").value(destinationTopic)
    .key("source").object()
        .key("type").value("s3")
        .key("config").object()
          .key("shard_path").value("data/homepages/samples/")
          .key("shard_prefix").value("part")
          
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
    _localKakfaService.sendMessage(SCHLOSS_SOURCE_QUEUE, message);
    List<String> readStringMessagesInTopic = _localKakfaService.readStringMessagesInTopic(SCHLOSS_SOURCE_QUEUE);
    _log.info("Messages in source_queue now are: " + Joiner.on("\n").join(readStringMessagesInTopic));
  }

  private void sendSinkMessage(String sinkTopic) {
    
    JSONBuilder builderSink = new JSONStringer();
    builderSink.object()
    .key("topic").value(sinkTopic)
    .key("sink").object()
        .key("type").value("s3")
        .key("shard_num").value(0)
        .key("config").object()
          .key("shard_path").value("test/worker_sink_service/" + sinkTopic + "/")
          .key("shard_prefix").value("test_shard_")
          .key("bucket").value("buffer.zillabyte.com")
          .key("credentials").object()
            .key("secret").value("")
            .key("access").value("")
          .endObject()
        .endObject()
      .endObject()
    .endObject();
    String sinkMessage = builderSink.toString();  
 
    _localKakfaService.sendMessage(SCHLOSS_SINK_QUEUE, sinkMessage);
    
  }

}
