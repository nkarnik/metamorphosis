package metamorphosis.test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import metamorphosis.kafka.LocalKafkaService;
import metamorphosis.schloss.SchlossService;
import metamorphosis.utils.Config;
import metamorphosis.utils.Utils;
import metamorphosis.workers.sinks.WorkerSinkService;
import metamorphosis.workers.sources.WorkerSourceService;
import net.sf.json.util.JSONBuilder;
import net.sf.json.util.JSONStringer;

import org.apache.log4j.Logger;
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

    Config.singleton().put("kafka.service", _localKakfaService);
    Config.singleton().put("kafka.consumer.timeout.ms", "1000");
    Config.singleton().put("kafka.zookeeper.connect", _localKakfaService.getZKConnectString());
    Config.singleton().put("gmb.zookeeper.connect", _localKakfaService.getZKConnectString());
    Config.singleton().put("kafka.brokers", Joiner.on(",").join(_localKakfaService.getSeedBrokers()));
    // GMB sends message to schloss topic
    // create SchlossService
    Config.singleton().put("schloss.source.queue", SCHLOSS_SOURCE_QUEUE);
    Config.singleton().put("schloss.sink.queue", SCHLOSS_SINK_QUEUE);
    Config.singleton().put("worker.source.queues", Joiner.on(",").join(_workerSourceQueues));
    Config.singleton().put("worker.sink.queues", Joiner.on(",").join(_workerSinkQueues));

    Config.singleton().put("worker.source.queue", _workerSourceQueues.get(0));
    Config.singleton().put("worker.sink.queue", _workerSinkQueues.get(0));

    
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
  public void testSourceMessage() throws InterruptedException, ExecutionException{
    JSONBuilder builder = new JSONStringer();
    builder.object()
    .key("topic").value(destinationTopic)
    .key("source").object()
        .key("type").value("s3")
        .key("config").object()
          .key("manifest").value("data/homepages/20140620.manifest.debug.small.one")
          .key("bucket").value("fatty.zillabyte.com")
          .key("credentials").object()
            .key("secret").value("")
            .key("access").value("")
          .endObject()
        .endObject()
      .endObject()
    .endObject();

    String message = builder.toString();
    _schlossService.start();
    _workerSourceService.start();
    Utils.sleep(5000);
    _localKakfaService.sendMessage(SCHLOSS_SOURCE_QUEUE, message);
//
//    _workerSourceService.setRunning(false);    
//    _schlossService.setRunning(false);
//    
//    _schlossService.startSourceReadThread()
//      .get();
//    _workerSourceService.startRoundRobinPushRead()
//      .get();
//    for(int i = 0; i < 2; i++){
//      _log.info("Popping round robin: " + i);
//      _workerSourceService.startRoundRobinPopThread()
//        .get();
//      _log.info("Waiting on Source worker to complete");
//      _workerSourceService.awaitTermination(3, TimeUnit.MINUTES);
//    }
//    _log.info("Waiting 100 seconds for test to complete");
    Utils.sleep(100 * 1000);
//    _log.info("Sleeping 15 seconds");
//    Utils.sleep(15 * 1000);
//    _log.info("");
//    _log.info("");
//    _log.info("Reading messages for confirmation for source phase");
//    _log.info("");
//    _log.info("");
//    
//    int numMessages = _localKakfaService.readNumMessages(destinationTopic);
//    _log.info("");
//    _log.info("Total messages on producer queues: " + numMessages);   
//    _log.info("");
//    assertEquals(1000, numMessages);
//    
//    try {
//      _log.info("Deleting temp s3 store");
//      S3Util.deletePath("buffer.zillabyte.com", "test/metamorphosis_test/");
//    } catch (S3ServiceException | S3Exception e1) {
//      _log.error("Deleting temp store failed: ", e1);
//    }    
//    
//    JSONBuilder builderSink = new JSONStringer();
//    builderSink.object()
//    .key("topic").value(destinationTopic)
//    .key("sink").object()
//        .key("type").value("s3")
//        .key("retry").value(0)
//        .key("config").object()
//          .key("shard_path").value("test/metamorphosis_test/")
//          .key("shard_prefix").value("test_shard_")
//          .key("bucket").value("buffer.zillabyte.com")
//          .key("credentials").object()
//            .key("secret").value("")
//            .key("access").value("")
//          .endObject()
//        .endObject()
//      .endObject()
//    .endObject();
//    String sinkMessage = builderSink.toString();    
//    _localKakfaService.sendMessage(SCHLOSS_SINK_QUEUE, sinkMessage);
//
//    _schlossService.startSinkReadThread()
//      .get();
//    
//    _workerSinkService.startRoundRobinPushRead()
//      .get();
//    for(int i = 0; i < 11; i++){
//      _log.info("Popping round robin: " + i);
//      _workerSinkService.startRoundRobinPopThread()
//        .get();
//      _log.info("Waiting on Sink worker to complete");
//      _workerSinkService.awaitTermination(3, TimeUnit.MINUTES);
//    }
//  
//    
//    _log.info("Sleeping 30 seconds ...");
//    Thread.sleep(30000);
//    
//    int totalSunk = 0;
//    
//    S3Object[] shards;
//    try {
//      shards = S3Util.listPath("buffer.zillabyte.com", "test/metamorphosis_test/");
//      for (S3Object shard: shards) {
//        String shardPath = shard.getKey();
//        String shardBucket = shard.getBucketName();
//        
//      try {
//        String[] sunk = S3Util.readGzipFile(shardBucket, shardPath).split("\n");
//        totalSunk += sunk.length;
//        _log.info("Received a total of " + totalSunk + " bytes");
//      } catch (IOException e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//      }
//        
//      }
//      
//    assertEquals(10000, totalSunk);
//      
//    } catch (S3ServiceException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
    
    
  }
  
}
