package metamorphosis.workers.sinks;

import net.sf.json.JSONObject;
import metamorphosis.workers.Worker;

public class WorkerS3Sink implements Worker {

  public WorkerS3Sink(JSONObject message) {
    // TODO Auto-generated constructor stub
  }

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

}
