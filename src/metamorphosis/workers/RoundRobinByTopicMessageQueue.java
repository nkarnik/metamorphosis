package metamorphosis.workers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import net.sf.json.JSONObject;

public class RoundRobinByTopicMessageQueue {
  
  private HashMap<String, ConcurrentLinkedQueue<JSONObject>> _queues;
  private int _remainingMessages;
  private int _queueNum;
  private ArrayList<String> _topics;

  public RoundRobinByTopicMessageQueue() {
    
    _queues = new HashMap<String, ConcurrentLinkedQueue<JSONObject>>();
    _queueNum = 1;
    _remainingMessages = 0;
    _topics = new ArrayList<String>();
    
    
  }
  
  public void push(JSONObject message) {
    
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
  
  public JSONObject pop() {
    if (_queues.size() <= 0) {
      return null;
    }
    
    String currentTopic = _topics.get(_queueNum % _topics.size());
    JSONObject popped =  _queues.get(currentTopic).poll();
    
    _queueNum += 1;
    if (popped != null) {
      _remainingMessages -= 1;
    }
    
    return popped;
  }
  
  
  
  
  
  

}
