package metamorphosis.workers.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import metamorphosis.kafka.LocalKafkaService;
import metamorphosis.workers.WorkerService;
import metamorphosis.workers.sources.WorkerSource;
import metamorphosis.workers.sources.WorkerSourceService;
import net.sf.json.util.JSONBuilder;
import net.sf.json.util.JSONStringer;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

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

  }

  @Test
  public void testSingleWorkerS3Source() throws InterruptedException, ExecutionException{

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
    _localKakfaService.sendMessage(_workerQueues.get(0), message);

    WorkerService<WorkerSource> workerService = new WorkerSourceService(_workerQueues.get(0), _localKakfaService);
    workerService.start();
    Thread.sleep(5000); // Give 10 seconds for the worker to get the message

    _log.info("Waiting on future...");
    workerService.stop(); // Awaits executor pool to finish
    
    _log.info("Reading messages for confirmation");
    _log.info("About to read from topic: " + DESTINATION_TOPIC);
    int messages = _localKakfaService.readNumMessages(DESTINATION_TOPIC);
    _log.info("There are " + messages + " messages in this queue");
    _log.info("Total messages on producer queues: " + messages);
    
    assertEquals(1000, messages);

  }
  
  

}
