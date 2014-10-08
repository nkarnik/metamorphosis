package metamorphosis.workers.sources;


public interface WorkerSource {
  
  public Iterable<String> getMessageIterator();

  public String getTopic();

}
