package metamorphosis.workers.sinks;

import java.io.File;

import kafka.consumer.ConsumerIterator;
import metamorphosis.workers.Worker;

import org.javatuples.Pair;

public abstract class WorkerSink implements Worker {

  @Override
  public Pair<File, Iterable<String>> getMessageIterator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getTopic() {
    // TODO Auto-generated method stub
    return null;
  }



  public abstract void sink(ConsumerIterator<String, String> sinkTopicIterator, int queueNumber);



}
