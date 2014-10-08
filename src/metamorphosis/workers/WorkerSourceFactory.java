package metamorphosis.workers;

import org.apache.commons.lang.NotImplementedException;

import metamorphosis.workers.sources.WorkerS3Source;
import metamorphosis.workers.sources.WorkerSource;
import net.sf.json.JSONObject;

public class WorkerSourceFactory {

  public static WorkerSource createSource(JSONObject message) {
    
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
