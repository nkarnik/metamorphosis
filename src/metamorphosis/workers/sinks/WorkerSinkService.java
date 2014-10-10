package metamorphosis.workers.sinks;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

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
import metamorphosis.utils.JSONDecoder;
import metamorphosis.utils.KafkaUtils;
import metamorphosis.workers.WorkerService;
import net.sf.json.JSONObject;

public class WorkerSinkService extends WorkerService<WorkerSink> {

  private Logger _log = Logger.getLogger(WorkerSinkService.class);

  public WorkerSinkService(String sourceTopic, KafkaService kafkaService) {
    super(sourceTopic, kafkaService, new WorkerSinkFactory());
  }

  @Override
  protected void processMessage(JSONObject poppedMessage) {
    // TODO What do we do with this popped message?
    WorkerSink workerSink = _workerFactory.createWorker(poppedMessage);
    String topic = poppedMessage.getString("topic");
    ConsumerIterator<String, String> sinkTopicIterator = getSinkTopicIterator(topic);
    workerSink.sink(sinkTopicIterator);
    int retry = 1 + poppedMessage.getJSONObject("sink").getInt("retry");
    poppedMessage.getJSONObject("sink").element("retry", retry);
    
    List<KeyedMessage<Integer, String>> messages = Lists.newArrayList();
    messages.add(new KeyedMessage<Integer,String>(_sourceTopic, poppedMessage.toString()));
    
    Properties properties = TestUtils.getProducerConfig(Joiner.on(',').join(_kafkaService.getSeedBrokers()), "kafka.producer.DefaultPartitioner");
    Producer<Integer, String> producer = new Producer<Integer,String>(new ProducerConfig(properties));
    producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
    _log.info("Sending message: " + poppedMessage.toString() + " to topic: " + _sourceTopic);
    producer.close(); 

  }
  
  private ConsumerIterator<String, String> getSinkTopicIterator(String topicName) {
    String clientName = topicName;
    ConsumerConfig consumerConfig = KafkaUtils.createConsumerConfig(_kafkaService.getZKConnectString(), clientName);
    ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topicName, new Integer(1)); // This consumer will only have one thread
    StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());
    KafkaStream<String,String> kafkaStream = consumer.createMessageStreams(topicCountMap, stringDecoder, stringDecoder).get(topicName).get(0);
    ConsumerIterator<String, String> iterator = kafkaStream.iterator();
    _log.info("Consumer " + clientName + " instantiated");
    return iterator;
  }
  
  
  
  

}
