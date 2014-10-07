package metamorphosis.utils;

import java.util.List;

public interface KafkaService {
  
  public List<String> getSeedBrokers();

  public void sendMessage(String topicQueue, String workerQueueMessage);

}
