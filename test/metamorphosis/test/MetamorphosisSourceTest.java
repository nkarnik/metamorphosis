package metamorphosis.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import metamorphosis.kafka.LocalKafkaService;
import metamorphosis.schloss.SchlossService;
import metamorphosis.utils.Config;
import metamorphosis.utils.s3.S3Exception;
import metamorphosis.utils.s3.S3Util;
import metamorphosis.workers.sinks.WorkerSinkService;
import metamorphosis.workers.sources.WorkerSourceService;
import net.sf.json.util.JSONBuilder;
import net.sf.json.util.JSONStringer;

import org.apache.log4j.Logger;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Object;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class MetamorphosisSourceTest {

  private static final int NUM_BROKERS = 1;
  private static final String SCHLOSS_SOURCE_QUEUE = "source_topic";
  private static final String SCHLOSS_SINK_QUEUE = "sink_topic";
  private static final String PRODUCER_QUEUE_PREFIX = "worker_source_queue_";
  private static final String CONSUMER_QUEUE_PREFIX = "worker_sink_queue_";
  private LocalKafkaService _localKakfaService;
  private List<String> _workerSourceQueues;
  private List<String> _workerSinkQueues;
  private SchlossService _schlossService;
  private WorkerSourceService _workerSourceService;
  private WorkerSinkService _workerSinkService;
  private String destinationTopic = "some_topic";
  
  private static Logger _log = Logger.getLogger(MetamorphosisSourceTest.class);

  @Before
  public void setup(){
    new Config();
    _workerSourceQueues = Lists.newArrayList();
    _workerSinkQueues = Lists.newArrayList();
    _localKakfaService = new LocalKafkaService(NUM_BROKERS);
    // Create required topics
    _localKakfaService.createTopic(SCHLOSS_SOURCE_QUEUE, 1, 1);
    _localKakfaService.createTopic(SCHLOSS_SINK_QUEUE, 1, 1);

    for (int i = 0; i < NUM_BROKERS; i++) {
      _workerSourceQueues.add(PRODUCER_QUEUE_PREFIX + i);
      _workerSinkQueues.add(CONSUMER_QUEUE_PREFIX + i);
      _localKakfaService.createTopic(PRODUCER_QUEUE_PREFIX + i, 1, 1);
      _localKakfaService.createTopic(CONSUMER_QUEUE_PREFIX + i, 1, 1);
    }



    // GMB sends message to schloss topic
    // create SchlossService
    Config.singleton().put("schloss.source.queue", SCHLOSS_SOURCE_QUEUE);
    Config.singleton().put("schloss.sink.queue", SCHLOSS_SINK_QUEUE);
    Config.singleton().put("kafka.zookeeper.connect", _localKakfaService.getZKConnectString());
    Config.singleton().put("kafka.brokers", Joiner.on(",").join(_localKakfaService.getSeedBrokers()));
    Config.singleton().put("worker.source.queues", Joiner.on(",").join(_workerSourceQueues));
    Config.singleton().put("worker.sink.queues", Joiner.on(",").join(_workerSinkQueues));

    _schlossService = new SchlossService();
    _workerSourceService = new  WorkerSourceService(_workerSourceQueues.get(0), _localKakfaService);
    _workerSinkService = new  WorkerSinkService(_workerSinkQueues.get(0), _localKakfaService);

  }
  
  @After
  public void tearDown(){
    _schlossService.stop();
    _workerSinkService.stop();
    _workerSourceService.stop();
    _localKakfaService.shutDown();
    
  }
  
  @Test
  public void testSourceMessage() throws InterruptedException{
    _workerSourceService.start();
    _schlossService.startSourceReadThread();
    JSONBuilder builder = new JSONStringer();
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
    _localKakfaService.sendMessage(SCHLOSS_SOURCE_QUEUE, message);
    _log.info("Sleeping 10 seconds");
    Thread.sleep(10000);
    _schlossService.stop();
    _workerSourceService.stop();
    
    _log.info("");
    _log.info("");
    _log.info("Reading messages for confirmation for source phase");
    _log.info("");
    _log.info("");
    
    int numMessages = _localKakfaService.readNumMessages(destinationTopic);
    _log.info("");
    _log.info("Total messages on producer queues: " + numMessages);   
    _log.info("");
    assertEquals(10000, numMessages);
    
    _schlossService.start();
    _workerSinkService.start();
    
    try {
      _log.info("Deleting temp s3 store");
      S3Util.deletePath("buffer.zillabyte.com", "test/metamorphosis_test/");
    } catch (S3ServiceException | S3Exception e1) {
      _log.error("Deleting temp store failed: ", e1);
    }    
    
    JSONBuilder builderSink = new JSONStringer();
    builderSink.object()
    .key("topic").value(destinationTopic)
    .key("sink").object()
        .key("type").value("s3")
        .key("retry").value(0)
        .key("config").object()
          .key("shard_path").value("test/metamorphosis_test/")
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
    _log.info("Sleeping 70 seconds ...");
    Thread.sleep(70000);
    
    _schlossService.stop();
    _workerSinkService.stop();
    
    int totalSunk = 0;
    
    S3Object[] shards;
    try {
      shards = S3Util.listPath("buffer.zillabyte.com", "test/metamorphosis_test/");
      for (S3Object shard: shards) {
        String shardPath = shard.getKey();
        String shardBucket = shard.getBucketName();
        
      try {
        String[] sunk = S3Util.readGzipFile(shardBucket, shardPath).split("\n");
        totalSunk += sunk.length;
        _log.info("Received a total of " + totalSunk + " bytes");
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
        
      }
      
    assertEquals(10000, totalSunk);
      
    } catch (S3ServiceException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    
  }
  
}