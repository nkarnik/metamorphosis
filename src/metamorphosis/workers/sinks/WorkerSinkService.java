package metamorphosis.workers.sinks;

import metamorphosis.kafka.KafkaService;
import metamorphosis.workers.WorkerService;
import net.sf.json.JSONObject;

public class WorkerSinkService extends WorkerService {

  public WorkerSinkService(String sourceTopic, KafkaService kafkaService) {
    super(sourceTopic, kafkaService, new WorkerSinkFactory());
  }

  @Override
  protected void processMessage(JSONObject poppedMessage) {
    // TODO What do we do with this popped message?
    

  }

}
