package metamorphosis.workers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;

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
    _queueNum = 0;
    _remainingMessages = 0;
    _topics = new ArrayList<String>();
    
    
  }
  
  public void push(MessageAndMetadata<String, JSONObject> messageAndMetadata) {
    
    JSONObject message = messageAndMetadata.message();

    String topic = message.getString("topic");
    ConcurrentLinkedQueue<JSONObject> queue = null;
    if (!_queues.containsKey(topic)) {
      _topics.add(topic);
      queue = new ConcurrentLinkedQueue<JSONObject>();
      _queues.put(topic, queue);
      _log.info("adding new topic to queues: " + topic + " q: " + queue);

    }else{
      queue = _queues.get(topic);
    }
    queue.add(message);
    _log.info("Adding to queue" + queue);
    
    _remainingMessages += 1;
    _log.info("Pushed message into round robin. Current remaining messages: " + _remainingMessages);
  }
  
  /**
   * Blocking pop
   * @return
   * @throws TimeoutException 
   */
  public JSONObject pop() throws TimeoutException {
    JSONObject popped = null;
    int topicsSeen = 0;

    int numWaits = 0;
    do {
      _log.info("POP from queue number: " + _queueNum );
  
      if(_topics.size() == 0){
        _log .info("No topics in the round robin, waiting...");

        Utils.sleep(1000);
        continue;
      }
      String currentTopic = _topics.get(_queueNum);

      ConcurrentLinkedQueue<JSONObject> concurrentLinkedQueue = _queues.get(currentTopic);
      popped =  concurrentLinkedQueue.poll();
      
      
      int size = _topics.size();
      _queueNum = (_queueNum == size - 1) ? 0 : _queueNum + 1;
      topicsSeen++;
      
      if(topicsSeen == size){
        numWaits++;
        if(numWaits > 10){
          throw new TimeoutException("Done waiting for a while to pop");
        }
        _log .info("No messages in the round robin, waiting...");
        topicsSeen = 0;
        Utils.sleep(1000);
      }
      
    } while (popped == null );
    _remainingMessages -= 1;
    _log.info("Popped message from round robin. Current remaining messages: " + _remainingMessages);

    return popped;
  }
  
  public int remainingMessages() {
    return _remainingMessages;
  }
  
  
  
  
  
  

}
