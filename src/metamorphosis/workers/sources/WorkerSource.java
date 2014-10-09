package metamorphosis.workers.sources;

import metamorphosis.workers.Worker;

public abstract class WorkerSource implements Worker {

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
