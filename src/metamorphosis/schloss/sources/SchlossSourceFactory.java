package metamorphosis.schloss.sources;

import metamorphosis.schloss.SchlossHandlerFactory;
import net.sf.json.JSONObject;

import org.apache.commons.lang.NotImplementedException;

public class SchlossSourceFactory implements SchlossHandlerFactory<SchlossSource>{

  public SchlossSource createSchlossHandler(JSONObject message) {
    String type = message.getJSONObject("source").getString("type");
    SchlossSource schlossSource = null;
    switch(type){
    case "s3":
      schlossSource = new SchlossS3Source(message);
      break;
    case "kinesis":
      
      break;
     default:
      throw new NotImplementedException("Cannot handle source of type: " + type); 

    }
    return schlossSource;
  }

  
}
