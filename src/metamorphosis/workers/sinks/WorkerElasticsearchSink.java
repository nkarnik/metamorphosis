/**
 * 
 */
package metamorphosis.workers.sinks;

import kafka.consumer.ConsumerIterator;

/**
 * @author sashi
 *
 */
public class WorkerElasticsearchSink extends WorkerSink {

  /**
   * 
   */
  public WorkerElasticsearchSink() {

  }

  @Override
  public void sink(ConsumerIterator<String, String> sinkTopicIterator, int queueNumber) {
    
  }

}
