package metamorphosis.schloss.sinks;

import metamorphosis.schloss.SchlossHandlerFactory;
import net.sf.json.JSONObject;

import org.apache.commons.lang.NotImplementedException;

public class SchlossSinkFactory implements SchlossHandlerFactory<SchlossSink>{

  @Override
  public SchlossSink createSchlossHandler(JSONObject message) {
    String type = message.getJSONObject("sink").getString("type");
    SchlossSink schlossSink = null;
    switch(type){
    case "s3":
      schlossSink = new SchlossS3Sink(message);
      break;
    case "kinesis":
      
      break;
     default:
      throw new NotImplementedException("Cannot handle source of type: " + type); 

    }
    return schlossSink;
  }

}
