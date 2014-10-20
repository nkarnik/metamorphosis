package metamorphosis.workers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.message.MessageAndMetadata;
import metamorphosis.utils.ExponentialBackoffTicker;
import metamorphosis.utils.Utils;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

public class RoundRobinByTopicMessageQueue {
  
  private HashMap<String, ConcurrentLinkedQueue<JSONObject>> _queues;
  private int _remainingMessages;
  private int _queueNum;
  private ArrayList<String> _topics;
  private Logger _log = Logger.getLogger(RoundRobinByTopicMessageQueue.class);
  private AtomicBoolean _interrupted;
  private ExponentialBackoffTicker _pushTicker = new ExponentialBackoffTicker(100);
  private ExponentialBackoffTicker _popTicker = new ExponentialBackoffTicker(100);
  private String _queueId;
  
  
  public RoundRobinByTopicMessageQueue(String queueId) {
    _queueId = queueId;
    
    _queues = new HashMap<String, ConcurrentLinkedQueue<JSONObject>>();
    _queueNum = 0;
    _remainingMessages = 0;
    _topics = new ArrayList<String>();
    _interrupted = new AtomicBoolean(false);
    
  }
  
  public void push(MessageAndMetadata<String, JSONObject> messageAndMetadata) {
    
    JSONObject message = messageAndMetadata.message();

    String topic = message.getString("topic");
    ConcurrentLinkedQueue<JSONObject> queue = null;
    if (!_queues.containsKey(topic)) {
      _topics.add(topic);
      queue = new ConcurrentLinkedQueue<JSONObject>();
      _queues.put(topic, queue);
      _log.info(_queueId + ":: adding new topic to queues: " + topic + " q: " + queue);

    }else{
      queue = _queues.get(topic);
    }
    queue.add(message);    
    _remainingMessages += 1;
    if(_popTicker.tick()){
      _log.info(_queueId + ":: [sampled #" + _popTicker.counter() + "] Pushed message into round robin. Current remaining messages: " + _remainingMessages);
      
    }
  }
  
  /**
   * Blocking pop
   * @return
   * @throws TimeoutException 
   */
  public JSONObject pop() throws TimeoutException {
    JSONObject popped = null;
    int topicsSeen = 0;

    do {
  
      if(_topics.size() == 0){
        _log.debug(_queueId + ":: No topics in the round robin, waiting...");
        Utils.sleep(1000);
        continue;
      }
      //_log.info("POP from queue number: " + _queueNum );
      String currentTopic = _topics.get(_queueNum);

      ConcurrentLinkedQueue<JSONObject> concurrentLinkedQueue = _queues.get(currentTopic);
      popped =  concurrentLinkedQueue.poll();

      int size = _topics.size();
      _queueNum = (_queueNum == size - 1) ? 0 : _queueNum + 1;
      topicsSeen++;
      
      if(topicsSeen == size){
        _log.debug(_queueId + ":: No messages in the round robin, waiting...");
        topicsSeen = 0;
        Utils.sleep(1000);
      }
      
    } while (popped == null && !_interrupted.get() );
    _remainingMessages -= 1;
    if(_pushTicker.tick()){
      _log.info(_queueId + ":: [sampled #" + _pushTicker.counter() + "] Popped message from round robin. Current remaining messages: " + _remainingMessages);
    }

    return popped;
  }
  
  public void interrupt(){
    _interrupted.set(true);
  }
  
  public int remainingMessages() {
    return _remainingMessages;
  }
  
  
  
  
  
  

}
