package metamorphosis.workers.sinks;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;
import kafka.utils.TestUtils;
import kafka.utils.VerifiableProperties;
import metamorphosis.kafka.KafkaService;
import metamorphosis.utils.ExponentialBackoffTicker;
import metamorphosis.utils.KafkaUtils;
import metamorphosis.workers.WorkerService;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class WorkerSinkService extends WorkerService<WorkerSink> {

  private Logger _log = Logger.getLogger(WorkerSinkService.class);
  private ConsumerConnector _consumer;
  HashMap<String, ConsumerIterator<String, String>> _topicToIteratorCache = Maps.newHashMap();
  ExponentialBackoffTicker _ticker = new ExponentialBackoffTicker(100);
  
  public WorkerSinkService(String sourceTopic, KafkaService kafkaService) {
    super(sourceTopic, kafkaService, new WorkerSinkFactory());
  }

  @Override
  protected void processMessage(JSONObject poppedMessage) {
    WorkerSink workerSink = _workerFactory.createWorker(poppedMessage);
    String topic = poppedMessage.getString("topic");
    String clientName = topic;
    ConsumerIterator<String, String> sinkTopicIterator;
    if(_topicToIteratorCache.containsKey(topic)){
      _log.debug("Using cached iterator for topic: " + topic);
      sinkTopicIterator = _topicToIteratorCache.get(topic);
    }else{
      ConsumerConfig consumerConfig = KafkaUtils.createConsumerConfig(_kafkaService.getZKConnectString(), clientName);
      _consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
      _log.info("New consumer created: " + _consumer.hashCode());
      
      Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
      topicCountMap.put(topic, new Integer(1)); // This consumer will only have one thread
      StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());
      
      sinkTopicIterator = _consumer.createMessageStreams(topicCountMap, stringDecoder, stringDecoder).get(topic).get(0).iterator();
      _log.info("Consumer " + clientName + " instantiated");
      _topicToIteratorCache.put(topic,sinkTopicIterator);
    }
    workerSink.sink(sinkTopicIterator, _queueNumber);
    
    //streaming sink, so have to increment retry and push back to worker queue
    int retry = 1 + poppedMessage.getJSONObject("sink").getInt("retry");
    poppedMessage.getJSONObject("sink").element("retry", retry);
    
    List<KeyedMessage<Integer, String>> messages = Lists.newArrayList();
    messages.add(new KeyedMessage<Integer,String>(_sourceTopic, poppedMessage.toString()));
    
    Properties properties = TestUtils.getProducerConfig(Joiner.on(',').join(_kafkaService.getSeedBrokers()), "kafka.producer.DefaultPartitioner");
    Producer<Integer, String> producer = new Producer<Integer,String>(new ProducerConfig(properties));
    producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
    if(_ticker.tick()){
      _log.info("[sampled #" + _ticker.counter() + "] Sending message: " + poppedMessage.toString() + " to topic: " + _sourceTopic);
    }
    producer.close(); 

  }
  
  
  
  

}
