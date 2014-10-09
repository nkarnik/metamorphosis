package metamorphosis.workers.sinks;

import metamorphosis.workers.WorkerFactory;
import net.sf.json.JSONObject;

import org.apache.commons.lang.NotImplementedException;


public class WorkerSinkFactory implements WorkerFactory<WorkerSink> {

  @Override
  public WorkerSink createWorker(JSONObject message) {
    String type = message.getJSONObject("sink").getString("type");
    WorkerSink workerSink = null;
    switch(type){
    case "s3":
      workerSink = new WorkerS3Sink(message);
      break;
    case "kinesis":
      
      break;
     default:
      throw new NotImplementedException("Cannot handle source of type: " + type); 

    }
    return workerSink;
    
  }

}
