package metamorphosis.workers.sinks;

import java.io.IOException;

import kafka.consumer.ConsumerIterator;
import net.sf.json.JSONObject;
import metamorphosis.workers.Worker;

public abstract class WorkerSink implements Worker {

  @Override
  public Iterable<String> getMessageIterator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getTopic() {
    // TODO Auto-generated method stub
    return null;
  }



  public abstract void sink(ConsumerIterator<String, String> sinkTopicIterator);



}
