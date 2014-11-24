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


  /**
   * Respond with number of tuples successfully flushed 
   * @param sinkTopicIterator
   * @param queueNumber
   * @return
   */
  public abstract int sink(ConsumerIterator<String, String> sinkTopicIterator, int queueNumber);



}
