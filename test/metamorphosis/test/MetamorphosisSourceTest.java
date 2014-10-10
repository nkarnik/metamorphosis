package metamorphosis.test;

import java.util.List;

import metamorphosis.kafka.LocalKafkaService;
import metamorphosis.schloss.SchlossService;
import metamorphosis.utils.Config;
import metamorphosis.workers.sinks.WorkerSinkService;
import metamorphosis.workers.sources.WorkerSourceService;
import net.sf.json.util.JSONBuilder;
import net.sf.json.util.JSONStringer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class MetamorphosisSourceTest {

  private static final int NUM_BROKERS = 2;
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

  @Before
  public void setup(){
    new Config();
    _workerSourceQueues = Lists.newArrayList();
    _workerSinkQueues = Lists.newArrayList();
    _localKakfaService = new LocalKafkaService(NUM_BROKERS);
    // Create required topics
    _localKakfaService.createTopic(SCHLOSS_SOURCE_QUEUE, 1, 1);
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
    
    _schlossService.start();
    _workerSinkService.start();
    _workerSourceService.start();
    
  }
  
  @After
  public void tearDown(){
    _schlossService.stop();
    _workerSinkService.stop();
    _workerSourceService.stop();
    _localKakfaService.shutDown();
    
  }
  
  @Test
  public void testSourceMessage(){
    
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
    _localKakfaService.sendMessage(SCHLOSS_SOURCE_QUEUE, message);
    
    
  }
  
  public void testSinkMessage(){
    
  }
  
}
