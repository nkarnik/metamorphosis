package metamorphosis.workers.sources;

import metamorphosis.workers.WorkerFactory;
import net.sf.json.JSONObject;

import org.apache.commons.lang.NotImplementedException;

public class WorkerSourceFactory implements WorkerFactory<WorkerSource> {

  public WorkerSourceFactory(){
    
  }
  
  @Override
  public WorkerSource createWorker(JSONObject message) {
    
    String type = message.getJSONObject("source").getString("type");
    WorkerSource workerSource = null;
    switch(type){
    case "s3":
      workerSource = new WorkerS3Source(message);
      break;
    case "kinesis":
      
      break;
     default:
      throw new NotImplementedException("Cannot handle source of type: " + type); 

    }
    return workerSource;
    
  }
  
}
