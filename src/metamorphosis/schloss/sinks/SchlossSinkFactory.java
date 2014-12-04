package metamorphosis.schloss.sinks;

import metamorphosis.schloss.SchlossHandlerFactory;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

public class SchlossSinkFactory implements SchlossHandlerFactory<SchlossSink>{

  private Logger _log = Logger.getLogger(SchlossSinkFactory.class);
  @Override
  public SchlossSink createSchlossHandler(JSONObject message) {
    String type = null;
    try{
      type = message.getJSONObject("sink").getString("type");
    }catch(JSONException e){
      _log.error("Expected sink message to contain sink object with a type string. Not found! : " + message.toString());
      return null;
    }
    SchlossSink schlossSink = null;
    switch(type){
    case "s3":
      schlossSink = new SchlossS3Sink(message);
      break;
    case "elasticsearch":
      schlossSink = new SchlossElasticsearchSink(message);
      break;
    case "kinesis":
      _log.error("Kinesis sources not supported.");
      break;
    default:
      _log.error("Cannot handle source of type: " + type); 

    }
    return schlossSink;
  }

}
