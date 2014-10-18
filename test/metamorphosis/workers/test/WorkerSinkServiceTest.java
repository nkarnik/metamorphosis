package metamorphosis.workers.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
  private Logger _log = Logger.getLogger(WorkerSourceServiceTest.class);
  private String TOPIC_TO_SINK = "more_test";

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
    _log.info("Test data added");
  }

  @Test
  public void testSingleWorkerS3Sink() throws InterruptedException, ExecutionException, S3ServiceException, S3Exception, IOException{
    
    JSONBuilder builder = new JSONStringer();
    String bucket = "buffer.zillabyte.com";
    String shardPath = "dev/single_worker_test/";
    S3Util.recursiveDeletePath(bucket, shardPath);
    
    builder.object()
    .key("topic").value(TOPIC_TO_SINK)
    .key("sink").object()
        .key("type").value("s3")
        .key("retry").value(0)
        .key("config").object()
          .key("shard_path").value(shardPath)
          .key("shard_prefix").value("test_shard_")
          .key("bucket").value(bucket)
          .key("credentials").object()
            .key("secret").value("")
            .key("access").value("")
          .endObject()
        .endObject()
      .endObject()
    .endObject();

    String message = builder.toString();
    String thisWorkerQueue = _workerQueues.get(0);
    _localKakfaService.sendMessage(thisWorkerQueue, message);

    // create SchlossService
    Config.singleton().put("worker.sink.topic", thisWorkerQueue);
    Config.singleton().put("kafka.zookeeper.connect", _localKakfaService.getZKConnectString());
    Config.singleton().put("gmb.zookeeper.connect", _localKakfaService.getZKConnectString());
    Config.singleton().put("kafka.brokers", Joiner.on(",").join(_localKakfaService.getSeedBrokers()));
    
    WorkerService<WorkerSink> workerService = new WorkerSinkService(thisWorkerQueue, _localKakfaService);
    workerService.start();
    Thread.sleep(3000); // Give 10 seconds for the worker to get the message

    _log.info("Waiting on future...");
    workerService.stop(); // Awaits executor pool to finish
    
    _log.info("Reading messages for confirmation");
    
    int numMessages = 0;
    S3Object[] shards = S3Util.listPath(bucket, shardPath);
    for(S3Object shard : shards){
      numMessages += S3Util.readGzipFile(bucket, shard.getKey()).split("\n").length;
    }
    assertEquals(BATCHES * PER_BATCH, numMessages);
 
  }
}
