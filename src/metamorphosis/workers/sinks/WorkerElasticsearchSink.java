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
        .put("client.transport.sniff", true).build();
    _client = new TransportClient(settings);
    for(String host : hosts){
      _client.addTransportAddress(new InetSocketTransportAddress(host, 9200)); // Default port
    }
    _topic = message.getString("topic");
  }

  @Override
  public void sink(ConsumerIterator<String, String> sinkTopicIterator, int queueNumber) {
    _log.info("Entering elasticsearch sink for topic: " + _topic);
    int sunk = 0;
    try{
      while (sinkTopicIterator.hasNext()) {
        MessageAndMetadata<String, String> fetchedMessage = sinkTopicIterator.next();
        String messageBody = fetchedMessage.message();
        IndexResponse response = _client.prepareIndex(_topic, "tuple")
            .setSource(messageBody)
            .execute()
            .actionGet();

        _log.debug("Elasticsearch returned : " + response.getId());
        sunk++;
      }
    }catch(ConsumerTimeoutException e){
      _log.info("Consumer timed out. ");  
    }
    _log.info("Exiting elasticsearch sink for topic: " + _topic + " after sinking " + sunk + " tuples");
    
  }

}
