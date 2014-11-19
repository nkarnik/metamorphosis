package metamorphosis.workers.sinks.test;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import metamorphosis.kafka.LocalKafkaService;
import metamorphosis.utils.Config;
import metamorphosis.utils.s3.S3Exception;
import metamorphosis.workers.WorkerService;
import metamorphosis.workers.sinks.WorkerSink;
import metamorphosis.workers.sinks.WorkerSinkService;
import net.sf.json.util.JSONBuilder;
import net.sf.json.util.JSONStringer;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.jets3t.service.S3ServiceException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class WorkerElasticsearchSinkTest {


  private static final int BATCHES = 100;
  private static final int PER_BATCH = 10;
  private ArrayList<String> _workerQueues;
  private LocalKafkaService _localKakfaService;
  private String CONSUMER_QUEUE_PREFIX = "consumer_queue_";
  private int NUM_BROKERS = 3;
  private Logger _log = Logger.getLogger(WorkerElasticsearchSinkTest.class);
  private String TOPIC_TO_SINK = "single_worker_sink";
  private Node _elasticsearchNode;
  private File _tempDir;

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
        messages.add("{\"message\":\"Message #" + batch * msgNum + "\", \"message_id\":\"" + batch * msgNum + "\"}");
      }
      _localKakfaService.sendMessageBatch(TOPIC_TO_SINK, messages);
    }
    
    _tempDir = Files.createTempDir();
    Settings settings = ImmutableSettings.settingsBuilder()
                          .put("path.data", _tempDir.getAbsolutePath())
                          .build();
    
    _elasticsearchNode = nodeBuilder()
                          .clusterName("elasticsearch")
                          .settings(settings)
                          .node();
    
    
    Config.singleton().put("kafka.service", _localKakfaService);
    Config.singleton().put("kafka.consumer.timeout.ms", "1000");
    Config.singleton().put("kafka.zookeeper.connect", _localKakfaService.getZKConnectString());
    Config.singleton().put("gmb.zookeeper.connect", _localKakfaService.getZKConnectString());
    Config.singleton().put("kafka.brokers", Joiner.on(",").join(_localKakfaService.getSeedBrokers()));
    Config.singleton().put("elasticsearch.hosts", "localhost");
    
    _log.info("Test data added");
  }
  
  @After
  public void teardown(){
    _log.info("Tearing down the tests");
    _localKakfaService.shutDown();
    _elasticsearchNode.close();
    FileUtils.deleteQuietly(_tempDir);
  }
  @Test
  public void testSingleWorkerElasticsearchSink() throws InterruptedException, ExecutionException, S3ServiceException, S3Exception, IOException{

    _log.info("Deleting elastic search index");
    
    
    
    JSONBuilder builderSink = new JSONStringer();
    builderSink.object()
    .key("topic").value(TOPIC_TO_SINK)
    .key("sink").object()
        .key("type").value("elasticsearch")
        .key("retry").value(0)
        .key("config").object()
          .key("index").value(TOPIC_TO_SINK)
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
  
    long totalSunk = 0;
    Settings settings = ImmutableSettings.settingsBuilder()
        .put("client.transport.sniff", true).build();
    TransportClient client = new TransportClient(settings);

    client.addTransportAddress(new InetSocketTransportAddress("localhost", 9300)); // Default port

    CountResponse response = client.prepareCount(TOPIC_TO_SINK)
        .setQuery(termQuery("_type", "tuple"))
        .execute()
        .actionGet();
    totalSunk = response.getCount();
    client.close();
    _log.info("Final number of messages in elasticsearch: " + totalSunk);
    assertEquals(BATCHES * PER_BATCH, totalSunk);
  }
}
