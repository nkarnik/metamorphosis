package metamorphosis.workers.sinks.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import metamorphosis.kafka.LocalKafkaService;
import metamorphosis.utils.Config;
import metamorphosis.utils.s3.S3Exception;
import metamorphosis.utils.s3.S3Util;
import metamorphosis.workers.WorkerService;
import metamorphosis.workers.sinks.WorkerSink;
import metamorphosis.workers.sinks.WorkerSinkService;
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

public class WorkerSinkServiceTest {
  private static final int BATCHES = 100;
  private static final int PER_BATCH = 10;
  private ArrayList<String> _workerQueues;
  private LocalKafkaService _localKakfaService;
  private String CONSUMER_QUEUE_PREFIX = "consumer_queue_";
  private int NUM_BROKERS = 3;
  private Logger _log = Logger.getLogger(WorkerSinkServiceTest.class);
  private String TOPIC_TO_SINK = "single_worker_sink";

  @Before
  public void setup() {
    _workerQueues = new ArrayList<String>();
    
    _localKakfaService = new LocalKafkaService(NUM_BROKERS);
    // Create required topics

    for (int i = 0; i < NUM_BROKERS; i++) {
      
      _workerQueues.add(CONSUMER_QUEUE_PREFIX  + i);
      _localKakfaService.createTopic(CONSUMER_QUEUE_PREFIX + i, 1, 1);
    }
    _localKakfaService.createTopic(TOPIC_TO_SINK, 10, 1);
    
    //Add test data
    _log.info("Adding test data");
    for(int batch = 0; batch < BATCHES; batch++){
      List<String> messages = Lists.newArrayList();
      for(int msgNum = 0; msgNum < PER_BATCH; msgNum++){
        messages.add("New test message: " + batch * msgNum);
      }
      _localKakfaService.sendMessageBatch(TOPIC_TO_SINK, messages);
    }
    
    Config.singleton().put("kafka.service", _localKakfaService);
    Config.singleton().put("kafka.consumer.timeout.ms", "1000");
    Config.singleton().put("kafka.zookeeper.connect", _localKakfaService.getZKConnectString());
    Config.singleton().put("gmb.zookeeper.connect", _localKakfaService.getZKConnectString());
    Config.singleton().put("kafka.brokers", Joiner.on(",").join(_localKakfaService.getSeedBrokers()));

    _log.info("Test data added");
  }

  @After
  public void teardown(){
    _localKakfaService.shutDown();
  }
  
  @Test(timeout=1000*200)
  public void testSingleWorkerS3Sink() throws InterruptedException, ExecutionException, S3ServiceException, S3Exception, IOException{
    
    try {
      _log.info("Deleting temp s3 store");
      S3Util.deletePath("buffer.zillabyte.com", "test/worker_sink_service/");
    } catch (S3ServiceException | S3Exception e1) {
      _log.error("Deleting temp store failed: ", e1);
    }    
    
    JSONBuilder builderSink = new JSONStringer();
    builderSink.object()
    .key("topic").value(TOPIC_TO_SINK)
    .key("sink").object()
        .key("type").value("s3")
        .key("retry").value(0)
        .key("config").object()
          .key("shard_path").value("test/worker_sink_service/" + TOPIC_TO_SINK + "/")
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
    _localKakfaService.sendMessage(CONSUMER_QUEUE_PREFIX + "1", sinkMessage);

    WorkerService<WorkerSink> workerSinkService = new WorkerSinkService(CONSUMER_QUEUE_PREFIX + "1", _localKakfaService);
    workerSinkService.setRunning(false);
    workerSinkService.startRoundRobinPushRead()
      .get();
    
    _log.info("Popping round robin: ");
    workerSinkService.startRoundRobinPopThread()
      .get();
    _log.info("Waiting on Source worker to complete");
    workerSinkService.awaitTermination(3, TimeUnit.MINUTES);
  
    int totalSunk = 0;
    
    S3Object[] shards;
    try {
      shards = S3Util.listPath("buffer.zillabyte.com", "test/worker_sink_service/" + TOPIC_TO_SINK + "/");
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
    } catch (S3ServiceException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    _log.info("Final number of messages on s3: " + totalSunk);
    assertEquals(BATCHES * PER_BATCH, totalSunk);
  }
}
