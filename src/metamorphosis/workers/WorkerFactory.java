package metamorphosis.workers;

import net.sf.json.JSONObject;


public interface WorkerFactory {

  public Worker createWorker(JSONObject message);
}
