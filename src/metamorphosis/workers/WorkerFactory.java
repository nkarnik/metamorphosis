package metamorphosis.workers;

import net.sf.json.JSONObject;


public interface WorkerFactory<T extends Worker> {

  public T createWorker(JSONObject message) ;
}
