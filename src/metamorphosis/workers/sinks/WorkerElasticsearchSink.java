/**
 * 
 */
package metamorphosis.workers.sinks;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.message.MessageAndMetadata;
import metamorphosis.utils.Config;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
/**
 * @author sashi
 *
 */
public class WorkerElasticsearchSink extends WorkerSink {

  TransportClient _client;
  private String _topic;
  private Logger _log = Logger.getLogger(WorkerElasticsearchSink.class);
  
  public WorkerElasticsearchSink(JSONObject message) {
    String hostsString = Config.singleton().getOrException("elasticsearch.hosts");
    String[] hosts = hostsString.split(",");
    Settings settings = ImmutableSettings.settingsBuilder()
                          .put("client.transport.sniff", true)
                          .build();
    _client = new TransportClient(settings);
    _log.info("Elasticsearch hosts are: " + hostsString + " Count: " + hosts.length);
    for(String host : hosts){
      _log.info("Adding host: " + host);
      _client.addTransportAddress(new InetSocketTransportAddress(host, 9300)); // Default port
    }
    _log.info("Total added hosts: " + _client.transportAddresses().size());
    _topic = message.getString("topic");
  }

  @Override
  public int sink(ConsumerIterator<String, String> sinkTopicIterator, int queueNumber) {
    _log.info("Entering elasticsearch sink for topic: " + _topic);
    int sunkTuples = 0;
    BulkRequestBuilder prepareBulk = _client.prepareBulk();
    try{
      while (sinkTopicIterator.hasNext()) {
        MessageAndMetadata<String, String> fetchedMessage = sinkTopicIterator.next();
        String messageBody = fetchedMessage.message();
        
        prepareBulk.add(_client.prepareIndex(_topic, "tuple")
            .setSource(messageBody));
        sunkTuples++;
        if(sunkTuples == 100){
          flush(prepareBulk);
          sunkTuples = 0;
          prepareBulk = _client.prepareBulk(); // Start new bulk sender
        }
      }
    }catch(ConsumerTimeoutException e){
      _log.info("Consumer timed out. ");  
      flush(prepareBulk);
    }
    _log.info("Exiting elasticsearch sink for topic: " + _topic + " after sinking " + sunkTuples + " tuples");
    return sunkTuples;
  }

  private void flush(BulkRequestBuilder prepareBulk) {
    _log.info("Flushing " + prepareBulk.numberOfActions() + " requests to elasticsearch");
    BulkResponse actionGet = prepareBulk.execute().actionGet();
    if(actionGet.hasFailures()){
      _log.error("Error while flushing to elasticsearch: ");
      for( BulkItemResponse response : actionGet.getItems()){
        if(response.isFailed()){
          _log.error("Failed: " + response.getFailureMessage());
        }
      }
    }
  }

}
