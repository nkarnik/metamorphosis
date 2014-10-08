package metamorphosis.workers.sources;

import java.util.List;

public interface WorkerSource {
  
  public List<String> getMessages();

  public String getTopic();

}
