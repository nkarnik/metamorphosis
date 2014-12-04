package metamorphosis.schloss.sources;

import metamorphosis.schloss.SchlossHandlerFactory;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

public class SchlossSourceFactory implements SchlossHandlerFactory<SchlossSource>{

  private Logger _log = Logger.getLogger(SchlossSourceFactory.class);

  public SchlossSource createSchlossHandler(JSONObject message) {
    String type = null;
    try{
      type = message.getJSONObject("source").getString("type");
    }catch(JSONException e){
      _log.error("Expected sink message to contain source object with a type string. Not found! : " + message.toString());
      return null;
    }
    SchlossSource schlossSource = null;
    switch(type){
    case "s3":
      schlossSource = new SchlossS3Source(message);
      break;
    case "kinesis":
      _log.error("Kinesis sources not supported.");
      break;
    default:
      _log.error("Cannot handle source of type: " + type); 

    }
    return schlossSource;
  }

  
}
