package metamorphosis.workers;

import java.util.List;

public class ShardProducerService {
  
  
  private List<String> _brokers;
  private String _zkConnectString;
  private String _sourceTopic;
  private List<String> _workerQueues;
  private int _queueToPush;

  public ShardProducerService(String sourceTopic, List<String> seedBrokers, String zkConnectString, List<String> workerQueues) {
    _brokers = seedBrokers;
    _zkConnectString = zkConnectString;
    _sourceTopic = sourceTopic;
    _workerQueues = workerQueues;
    _queueToPush = 0;
  }

}
