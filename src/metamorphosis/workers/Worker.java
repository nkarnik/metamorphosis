package metamorphosis.workers;


public interface Worker {
  
  public Iterable<String> getMessageIterator();

  public String getTopic();

}
