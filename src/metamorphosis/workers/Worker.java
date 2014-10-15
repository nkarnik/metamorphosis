package metamorphosis.workers;

import java.io.File;

import org.javatuples.Pair;


public interface Worker {
  
  public Pair<File, Iterable<String>> getMessageIterator();

  public String getTopic();

}
