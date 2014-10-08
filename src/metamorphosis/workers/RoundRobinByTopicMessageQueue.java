package metamorphosis.workers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import kafka.message.MessageAndMetadata;
import metamorphosis.utils.Utils;
import net.sf.json.JSONObject;

public class RoundRobinByTopicMessageQueue {
  
  private HashMap<String, ConcurrentLinkedQueue<JSONObject>> _queues;
  private int _remainingMessages;
  private int _queueNum;
  private ArrayList<String> _topics;
  private Logger _log = Logger.getLogger(RoundRobinByTopicMessageQueue.class);

  public RoundRobinByTopicMessageQueue() {
    
    _queues = new HashMap<String, ConcurrentLinkedQueue<JSONObject>>();
    _queueNum = 1;
    _remainingMessages = 0;
    _topics = new ArrayList<String>();
    
    
  }
  
  public void push(MessageAndMetadata<String, JSONObject> messageAndMetadata) {
    
    JSONObject message = messageAndMetadata.message();
    
    boolean found = false;
    String topic = message.getJSONObject("message").getString("topic");
    if (_queues.containsKey(topic)) {
      found = true;
      ConcurrentLinkedQueue<JSONObject> queue = _queues.get(topic);
      queue.add(message);
    }
    
    if (found == false) {
      _topics.add(topic);
      ConcurrentLinkedQueue<JSONObject> queue = new ConcurrentLinkedQueue<JSONObject>();
      queue.add(message);
      _queues.put(topic, queue);
    }
    _remainingMessages += 1;
  }
  
  /**
   * Blocking pop
   * @return
   */
  public JSONObject pop() {
    if (_queues.size() == 0) {
      return null;
    }
    
    JSONObject popped = null;
    int topicsSeen = 0;
    
    do {
      String currentTopic = _topics.get(_queueNum);
      ConcurrentLinkedQueue<JSONObject> concurrentLinkedQueue = _queues.get(currentTopic);
      popped =  concurrentLinkedQueue.poll();
      
      int size = _topics.size();
      _queueNum = (_queueNum == size - 1) ? 0 : _queueNum + 1;
      topicsSeen++;
      
      if(topicsSeen == size){
        _log .info("No messages in the round robin, waiting...");
        Utils.sleep(1000);
      }
    } while (popped == null );
    
    if (popped != null) {
      _remainingMessages -= 1;
    }
    
    return popped;
  }
  
  public int remainingMessages() {
    return _remainingMessages;
  }
  
  
  
  
  
  

}
