package metamorphosis.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;

public class KafkaUtils {

  private static Logger _log = Logger.getLogger(KafkaUtils.class);
  /**
   * Gets the offset of the partition for THIS consumer, identified by the clientName. 
   * Allows resuming!
   * @param consumer
   * @param topic
   * @param partition
   * @param whichTime
   * @param clientName
   * @return
   */
  public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
    OffsetResponse response = getOffsetsBefore(consumer, topic, partition, whichTime, clientName);
    long[] offsets = response.offsets(topic, partition);
    return offsets[0];
  }

  public static OffsetResponse getOffsetsBefore(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 10));
    kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
    OffsetResponse response = consumer.getOffsetsBefore(request);
    
    
    if (response.hasError()) {
      _log.info("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
      return null;
    }
    return response;
  }

  public static Broker findLeader(List<String> seedBrokers, String topic, int partition) {
    return findPartitionMetadata(seedBrokers, topic, partition).leader();
  }
  
  public static PartitionMetadata findPartitionMetadata(List<String> seedBrokers, String topic, int partition) {
    PartitionMetadata returnMetaData = null;
    loop: for (String seed : seedBrokers) {
      SimpleConsumer consumer = null;
      try {
        String[] split = seed.split(":");
        consumer = new SimpleConsumer(split[0], Integer.parseInt(split[1]), 100000, 64 * 1024, "leaderLookup");
        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        TopicMetadataResponse resp = consumer.send(req);

        List<TopicMetadata> metaData = resp.topicsMetadata();
        for (TopicMetadata item : metaData) {
          for (PartitionMetadata part : item.partitionsMetadata()) {
            if (part.partitionId() == partition) {
              returnMetaData = part;
              break loop;
            }
          }
        }
      } catch (Exception e) {
        _log.info("Error communicating with Broker [" + seed + "] to find Leader for [" + topic + ", " + partition + "] Reason: " + e);
      } finally {
        if (consumer != null)
          consumer.close();
      }
    }
    if (returnMetaData == null){
      throw new RuntimeException("No partition (" + partition + ") found for " + topic);
    }
    return returnMetaData;
  }
  

  public static long readAllPartitions(List<String> brokerList,  String topic, int partitions) throws UnsupportedEncodingException {
    long numResults = 0;
    int timeout = 10000;
    int bufferSize = 10 * 1000 * 1000;
    int fetchSize = 10 * 1000 * 1000;
    String consumerClientName = "reader";
    
    for(int partition = 0; partition < partitions; partition++){
      
      _log.info("Reading partition: " + partition);
      PartitionMetadata partitionMetadata = KafkaUtils.findPartitionMetadata(brokerList, topic, partition);
      if(partitionMetadata == null){
        _log.error("Topic has no partitions, verify topic setup");
        continue;
      }
      SimpleConsumer consumer = new SimpleConsumer(partitionMetadata.leader().host(), partitionMetadata.leader().port(), timeout, bufferSize, consumerClientName);

      OffsetResponse offsetResponse = getOffsetsBefore(consumer, topic, partition, OffsetRequest.EarliestTime(), "lastOffsetReader");
      long[] offsets = offsetResponse.offsets(topic, partition);
      // numResults += lastOffset;
      long readOffset = offsets[0];
      _log.info("offset for partition: " + partition + " :: " + readOffset);

      int numReads = 0;
      // An arbitrary number of reads are required to complete a partition
      while(true){
        FetchRequest req = new FetchRequestBuilder().clientId(consumerClientName)
            .addFetch(topic, partition, readOffset, fetchSize) 
            .build();
        FetchResponse fetchResponse = consumer.fetch(req);
        
        if(fetchResponse.hasError()){
          _log.info("Error in fetch:: " + fetchResponse.errorCode(topic, partition));
          continue;
        }
        ByteBufferMessageSet messageSet = fetchResponse.messageSet(topic, partition);
        _log.info("messageSet: " + messageSet.sizeInBytes());
        Iterator<MessageAndOffset> iterator = messageSet.iterator();
        if(iterator.hasNext()){
          MessageAndOffset next = iterator.next();
          long nextOffset = next.nextOffset();
          _log.info("Next offset of first message: " + nextOffset);  
        }else {
          _log.info("No more messages");
          break;
        }
        
        // Each fetch request consists of several messages of max size fetchSize.
        for (MessageAndOffset messageAndOffset : messageSet) {
          long currentOffset = messageAndOffset.offset();
          if (currentOffset < readOffset) {
            _log.info("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
            continue;
          }
          readOffset = messageAndOffset.nextOffset();
          _log.info(" nextOffset " + readOffset);

          ByteBuffer payload = messageAndOffset.message().payload();
          byte[] bytes = new byte[payload.limit()];
          payload.get(bytes);
          BufferedReader bf;
          try {
            bf = new BufferedReader(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(bytes)), "UTF-8"));
            StringBuilder outStr = new StringBuilder();
            String line;
            while ((line=bf.readLine())!=null) {
              outStr.append(line);
            }
            String record = outStr.toString(); // Maybe ascii is sufficient?
            _log.info("Message found: " + record.substring(0, 50) + "...");
          } catch (IOException e) {
            String record = new String(bytes, "UTF-8");
            _log.info("Message found: " + record.substring(0, 50) + "...");
          }
          
          
          numResults += 1;
        }
        
        if(messageSet.sizeInBytes() == 0 || messageSet.sizeInBytes() < fetchSize){
          // Note. this works because we assume that no data is being pushed in while we're consuming,
          // Isn't true always.
          break; // Done with partition
        }
        numReads += 1;
      }
      _log.info("Partition complete: " + partition + " Reads completed: " + numReads);
      consumer.close();
    }
    return numResults;
  }

  
  public static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
    Properties props = getDefaultProperties(zookeeper, groupId);
    
    return new ConsumerConfig(props);
  }

  public static Properties getDefaultProperties(String zookeeper, String groupId) {
    Properties props = new Properties();
    props.put("zookeeper.connect", zookeeper);
    props.put("group.id", groupId);
    props.put("zookeeper.session.timeout.ms", "4000");
    props.put("zookeeper.sync.time.ms", "2000");
    props.put("auto.commit.interval.ms", "10000");
    props.put("auto.offset.reset", "smallest");
    props.put("consumer.timeout.ms", "500");
    return props;
  }
  

}
