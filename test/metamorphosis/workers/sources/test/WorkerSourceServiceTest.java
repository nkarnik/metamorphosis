package metamorphosis.workers.sources.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import metamorphosis.kafka.LocalKafkaService;
import metamorphosis.utils.Config;
import metamorphosis.workers.WorkerService;
import metamorphosis.workers.sources.WorkerSource;
import metamorphosis.workers.sources.WorkerSourceService;
import net.sf.json.util.JSONBuilder;
import net.sf.json.util.JSONStringer;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;

public class WorkerSourceServiceTest {
  
  private ArrayList<String> _workerQueues;
  private LocalKafkaService _localKakfaService;
  private String PRODUCER_QUEUE_PREFIX = "producer_queue_";
  private int NUM_BROKERS = 3;
  private Logger _log = Logger.getLogger(WorkerSourceServiceTest.class);
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


    Config.singleton().put("kafka.service", _localKakfaService);
    Config.singleton().put("kafka.consumer.timeout.ms", "1000");
    Config.singleton().put("kafka.zookeeper.connect", _localKakfaService.getZKConnectString());
    Config.singleton().put("gmb.zookeeper.connect", _localKakfaService.getZKConnectString());
    Config.singleton().put("kafka.brokers", Joiner.on(",").join(_localKakfaService.getSeedBrokers()));

  }
  
  @After
  public void teardown(){
    _localKakfaService.shutDown();
  }

  @Test(timeout=1000*200)
  public void testSingleWorkerS3Source() throws InterruptedException, ExecutionException{

    JSONBuilder builder = new JSONStringer();
    builder.object()
    .key("topic").value(DESTINATION_TOPIC)
    .key("source").object()
        .key("type").value("s3")
        .key("config").object()
          .key("shard_path").value("data/homepages/2014/0620/1013632003/part_0002.gz")
          .key("shard_prefix").value("part_")
          .key("bucket").value("fatty.zillabyte.com")
          .key("credentials").object()
            .key("secret").value("")
            .key("access").value("")
          .endObject()
        .endObject()
      .endObject()
    .endObject();

    String message = builder.toString();
    _localKakfaService.sendMessage(_workerQueues.get(0), message);

    WorkerService<WorkerSource> workerSourceService = new WorkerSourceService(_workerQueues.get(0), _localKakfaService);
    workerSourceService.setRunning(false);
    workerSourceService.startRoundRobinPushRead()
      .get();
    workerSourceService.startRoundRobinPopThread()
      .get();
    workerSourceService.awaitTermination(3, TimeUnit.MINUTES);

    
    
    _log.info("Reading messages for confirmation");
    _log.info("About to read from topic: " + DESTINATION_TOPIC);
    long messages = _localKakfaService.getTopicMessageCount(DESTINATION_TOPIC);
    _log.info("There are " + messages + " messages in this queue");
    _log.info("Total messages on producer queues: " + messages);
    
    assertEquals(966, messages); // Some skipped due to large size

  }
  
  

}
