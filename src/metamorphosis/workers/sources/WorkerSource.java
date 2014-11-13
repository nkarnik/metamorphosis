package metamorphosis.workers.sources;

import java.io.File;

import metamorphosis.workers.Worker;

import org.javatuples.Pair;

public abstract class WorkerSource implements Worker {

  @Override
  public Pair<File,Iterable<String>> getMessageIterator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getTopic() {
    // TODO Auto-generated method stub
    return null;
  }

}
