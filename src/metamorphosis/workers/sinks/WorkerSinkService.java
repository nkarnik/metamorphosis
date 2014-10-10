package metamorphosis.workers.sinks;

import metamorphosis.kafka.KafkaService;
import metamorphosis.workers.WorkerService;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class WorkerSinkService extends WorkerService<WorkerSink> {

  public WorkerSinkService(String sourceTopic, KafkaService kafkaService) {
    super(sourceTopic, kafkaService, new WorkerSinkFactory());
  }

  @Override
  protected void processMessage(JSONObject poppedMessage) {
    // TODO What do we do with this popped message?
    

  }

}
